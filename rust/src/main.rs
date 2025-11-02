use std::cell::RefCell;
use std::collections::HashMap;
use std::env;
use std::io::{ BufRead, BufReader };
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{ Child, Command, Stdio };
use std::rc::Rc;
use std::thread;
use std::time::Duration;

use serde_json::Value;
use tao::{
    dpi::LogicalSize,
    event::{ Event, WindowEvent },
    event_loop::{ ControlFlow, EventLoop },
    window::WindowBuilder,
};
use wry::{ Rect, Result, WebView, WebViewBuilder };

fn main() -> Result<()> {
    let backend_exe = if cfg!(target_os = "windows") { "galleria.exe" } else { "./galleria" };
    eprintln!("starting backend executable: {}", backend_exe);
    if std::path::Path::new(&backend_exe).exists() {
        eprintln!("backend path exists: {}", backend_exe);
    } else {
        eprintln!("warning: backend path not found relative to cwd: {}", backend_exe);
    }

    let mut backend: Child = Command::new(backend_exe)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to start backend");

    if let Some(stdout) = backend.stdout.take() {
        thread::spawn(move || {
            let reader = BufReader::new(stdout);
            for line in reader.lines().flatten() {
                eprintln!("[backend stdout] {}", line);
            }
        });
    }
    if let Some(stderr) = backend.stderr.take() {
        thread::spawn(move || {
            let reader = BufReader::new(stderr);
            for line in reader.lines().flatten() {
                eprintln!("[backend stderr] {}", line);
            }
        });
    }

    let mut backend_ready = false;
    for _ in 0..25 {
        if TcpStream::connect(("127.0.0.1", 3000)).is_ok() {
            backend_ready = true;
            break;
        }
        match backend.try_wait() {
            Ok(Some(status)) => {
                eprintln!("backend exited early with status: {:?}", status);
                break;
            }
            _ => {}
        }
        thread::sleep(Duration::from_millis(200));
    }
    if !backend_ready {
        eprintln!("backend did not respond on http://127.0.0.1:3000 after timeout");
    } else {
        eprintln!("backend listening on http://127.0.0.1:3000");
    }

    let event_loop = EventLoop::new();
    let appdata_dir = dirs
        ::data_local_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("Galleria");
    std::fs::create_dir_all(&appdata_dir).expect("Failed to create AppData folder");
    if cfg!(target_os = "windows") {
        env::set_var("WEBVIEW2_USER_DATA_FOLDER", &appdata_dir);
    }

    let initial_url = "http://localhost:3000".to_string();

    let window = WindowBuilder::new()
        .with_title("Galleria")
        .with_inner_size(LogicalSize::new(1024.0, 768.0))
        .build(&event_loop)
        .unwrap();

    let toolbar_html =
        r####"<!doctype html>
<html>
<head>
<meta charset='utf-8'>
<meta name='viewport' content='width=device-width,initial-scale=1'/>
<style>
html,body { height:100%; margin:0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial; background:#0f0f0f; color:#ddd; overflow:hidden; }
#bar { display:flex; align-items:center; padding:6px; gap:8px; height:48px; box-sizing:border-box; }
button { padding:6px 10px; border:1px solid #333; background:#1b1b1b; color:#ddd; border-radius:6px; cursor:pointer; }
input { flex:1; padding:6px 10px; border:1px solid #333; background:#141414; color:#eee; border-radius:6px; outline:none; }
#tabs { display:flex; gap:4px; overflow-x:auto; margin-left:8px; }
</style>
</head>
<body>
  <div id='bar'>
    <button id='back'>&larr;</button>
    <button id='forward'>&rarr;</button>
    <input id='url' type='text' placeholder='Enter URL' />
    <button id='go'>Go</button>
    <button id='new'>+ Tab</button>
    <div id='tabs'></div>
  </div>
  <script>
  function send(obj){ window.ipc.postMessage(JSON.stringify(obj)); }

  document.getElementById('go').onclick = ()=> send({type:'navigate', url: document.getElementById('url').value});
  document.getElementById('new').onclick = ()=> send({type:'new_tab'});
  document.getElementById('url').addEventListener('keydown', e=>{
    if(e.key==='Enter') send({type:'navigate', url: document.getElementById('url').value});
  });

  function renderTabs(tabs, active) {
    const c = document.getElementById('tabs');
    c.innerHTML = '';
    for (const t of tabs) {
      const b = document.createElement('button');
      try {
        const u = new URL(t.url || '', 'http://localhost');
        b.textContent = u.hostname || 'Tab';
      } catch(e) {
        b.textContent = 'Tab';
      }
      b.style.background = t.id === active ? '#333' : '#1b1b1b';
      b.onclick = () => send({type:'switch_tab', tabId: t.id});
      c.appendChild(b);
    }
  }

  window.host_update = function(payloadJson){
    try{
      const payload = JSON.parse(payloadJson);
      const tabs = payload.tabs || [];
      const active = payload.active;
      const activeTab = tabs.find(x=>x.id===active);
      document.getElementById('url').value = activeTab ? (activeTab.url || '') : '';
      renderTabs(tabs, active);
    }catch(e){}
  }
  </script>
</body>
</html>"####.to_string();

    let tabs: Rc<RefCell<HashMap<u64, WebView>>> = Rc::new(RefCell::new(HashMap::new()));
    let active_tab: Rc<RefCell<Option<u64>>> = Rc::new(RefCell::new(None));
    let next_tab_id: Rc<RefCell<u64>> = Rc::new(RefCell::new(1));
    let toolbar_handle: Rc<RefCell<Option<WebView>>> = Rc::new(RefCell::new(None));
    let pending_cmds: Rc<RefCell<Vec<Value>>> = Rc::new(RefCell::new(Vec::new()));
    let toolbar_ipc = {
        let cmds = pending_cmds.clone();
        move |req: wry::http::Request<String>| {
            if let Ok(v) = serde_json::from_str::<Value>(req.body()) {
                cmds.borrow_mut().push(v);
            }
        }
    };
    fn update_toolbar(
        toolbar: &Option<WebView>,
        tabs: &HashMap<u64, WebView>,
        active: Option<u64>
    ) {
        if let Some(tb) = toolbar {
            let mut list = vec![];
            for (id, wv) in tabs.iter() {
                let url = wv.url().unwrap_or_else(|_| "".to_string());
                list.push(serde_json::json!({"id": id, "url": url}));
            }
            let payload = serde_json::json!({ "tabs": list, "active": active });
            let _ = tb.evaluate_script(&format!("window.host_update('{}')", payload.to_string()));
        }
    }
    let toolbar_height: f64 = 48.0;
    let win_size = window.inner_size();
    let win_w = win_size.width as f64;
    let win_h = win_size.height as f64;
    let toolbar_bounds = Rect {
        x: 0,
        y: 0,
        width: win_w as u32,
        height: toolbar_height as u32,
    };
    let content_bounds = Rect {
        x: 0,
        y: toolbar_height as i32,
        width: win_w as u32,
        height: (win_h - toolbar_height).max(0.0) as u32,
    };

    let initial_id = {
        let mut n = next_tab_id.borrow_mut();
        let v = *n;
        *n += 1;
        v
    };
    let _toolbar_ref = toolbar_handle.clone();
    let _tabs_ref = tabs.clone();
    let _active_ref = active_tab.clone();
    let initial_wv = WebViewBuilder::new_as_child(&window)
        .with_bounds(content_bounds)
        .with_url(&initial_url)
        .build()?;
    tabs.borrow_mut().insert(initial_id, initial_wv);
    *active_tab.borrow_mut() = Some(initial_id);
    let toolbar_webview = WebViewBuilder::new_as_child(&window)
        .with_bounds(toolbar_bounds)
        .with_html(&toolbar_html)
        .with_ipc_handler(toolbar_ipc)
        .build()?;
    toolbar_handle.borrow_mut().replace(toolbar_webview);
    update_toolbar(&*toolbar_handle.borrow(), &tabs.borrow(), Some(initial_id));
    event_loop.run(move |event, _, control_flow| {
        *control_flow = ControlFlow::Wait;
        let mut cmds_ref = pending_cmds.borrow_mut();
        if !cmds_ref.is_empty() {
            let items: Vec<_> = cmds_ref.drain(..).collect();
            drop(cmds_ref);

            for v in items {
                if let Some(t) = v.get("type").and_then(|s| s.as_str()) {
                    match t {
                        "new_tab" => {
                            let id = {
                                let mut n = next_tab_id.borrow_mut();
                                let v = *n;
                                *n += 1;
                                v
                            };

                            let _toolbar_ref = toolbar_handle.clone();
                            let _tabs_ref = tabs.clone();
                            let _active_ref = active_tab.clone();

                            let wv = WebViewBuilder::new_as_child(&window)
                                .with_bounds(content_bounds)
                                .with_url(&initial_url)
                                .build();

                            match wv {
                                Ok(wv) => {
                                    for (_id, other_wv) in tabs.borrow_mut().iter_mut() {
                                        let _ = other_wv.set_visible(false);
                                    }
                                    let _ = wv.set_visible(true);
                                    tabs.borrow_mut().insert(id, wv);
                                    *active_tab.borrow_mut() = Some(id);
                                    update_toolbar(
                                        &*toolbar_handle.borrow(),
                                        &tabs.borrow(),
                                        Some(id)
                                    );
                                }
                                Err(e) => eprintln!("failed to create child webview: {:?}", e),
                            }
                        }
                        "navigate" => {
                            if let Some(url) = v.get("url").and_then(|s| s.as_str()) {
                                if let Some(active) = *active_tab.borrow() {
                                    if let Some(wv) = tabs.borrow_mut().get_mut(&active) {
                                        let _ = wv.load_url(url);
                                        update_toolbar(
                                            &*toolbar_handle.borrow(),
                                            &tabs.borrow(),
                                            Some(active)
                                        );
                                    }
                                }
                            }
                        }
                        "switch_tab" => {
                            if let Some(tab_id) = v.get("tabId").and_then(|n| n.as_u64()) {
                                for (id, wv) in tabs.borrow_mut().iter_mut() {
                                    let _ = wv.set_visible(*id == tab_id);
                                }
                                *active_tab.borrow_mut() = Some(tab_id);
                                update_toolbar(
                                    &*toolbar_handle.borrow(),
                                    &tabs.borrow(),
                                    Some(tab_id)
                                );
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
        match event {
            Event::WindowEvent { event: WindowEvent::Resized(new_size), .. } => {
                let toolbar_height = 48;
                let toolbar_bounds = Rect {
                    x: 0,
                    y: 0,
                    width: new_size.width,
                    height: toolbar_height,
                };
                let content_bounds = Rect {
                    x: 0,
                    y: toolbar_height as i32,
                    width: new_size.width,
                    height: new_size.height.saturating_sub(toolbar_height),
                };
                for (_id, wv) in tabs.borrow_mut().iter_mut() {
                    let _ = wv.set_bounds(content_bounds);
                }
                if let Some(tb) = &*toolbar_handle.borrow() {
                    let _ = tb.set_bounds(toolbar_bounds);
                }
            }
            Event::WindowEvent { event: WindowEvent::CloseRequested, .. } => {
                let _ = backend.kill();
                *control_flow = ControlFlow::Exit;
            }
            _ => {}
        }
    });
}
