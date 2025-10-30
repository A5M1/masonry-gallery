use std::io::{BufRead, BufReader};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;
use std::{
    env,
    path::PathBuf,
    process::{Child, Command, Stdio},
};

use tao::{
    dpi::LogicalSize,
    event::{Event, WindowEvent},
    event_loop::{ControlFlow, EventLoop},
    window::WindowBuilder,
};
use wry::{Rect, Result, WebViewBuilder};

fn main() -> Result<()> {
    let backend_exe = if cfg!(target_os = "windows") {
        "galleria.exe"
    } else {
        "./galleria"
    };
    eprintln!("starting backend executable: {}", backend_exe);
    if std::path::Path::new(&backend_exe).exists() {
        eprintln!("backend path exists: {}", backend_exe);
    } else {
        eprintln!(
            "warning: backend path not found relative to cwd: {}",
            backend_exe
        );
    }

    let mut backend: Child = Command::new(backend_exe)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to start backend");

    if let Some(stdout) = backend.stdout.take() {
        let mut reader = BufReader::new(stdout);
        thread::spawn(move || {
            let mut line = String::new();
            while let Ok(n) = reader.read_line(&mut line) {
                if n == 0 {
                    break;
                }
                eprintln!("[backend stdout] {}", line.trim_end());
                line.clear();
            }
        });
    }
    if let Some(stderr) = backend.stderr.take() {
        let mut reader = BufReader::new(stderr);
        thread::spawn(move || {
            let mut line = String::new();
            while let Ok(n) = reader.read_line(&mut line) {
                if n == 0 {
                    break;
                }
                eprintln!("[backend stderr] {}", line.trim_end());
                line.clear();
            }
        });
    }

    let mut backend_ready = false;
    for _ in 0..25 {
        if let Ok(_) = TcpStream::connect(("127.0.0.1", 3000)) {
            backend_ready = true;
            break;
        }
        match backend.try_wait() {
            Ok(Some(status)) => {
                eprintln!("backend exited early with status: {:?}", status);
                break;
            }
            Ok(None) => {}
            Err(e) => {
                eprintln!("error checking backend status: {:?}", e);
                break;
            }
        }
        thread::sleep(Duration::from_millis(200));
    }
    if !backend_ready {
        eprintln!("backend did not respond on http://127.0.0.1:3000 after timeout");
    } else {
        eprintln!("backend listening on http://127.0.0.1:3000");
    }

    let event_loop = EventLoop::new();

    let appdata_dir = dirs::data_local_dir()
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

    let toolbar_html = r####"<!doctype html>
    <html>
    <head>
    <meta charset='utf-8'>
    <meta name='viewport' content='width=device-width,initial-scale=1'/>
    <style>
    html,body { height:100%; margin:0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial; background:#0f0f0f; color:#ddd; overflow:hidden; }
    #bar { display:flex; align-items:center; padding:6px; gap:8px; height:48px; box-sizing:border-box; }
    button { padding:6px 10px; border:1px solid #333; background:#1b1b1b; color:#ddd; border-radius:6px; cursor:pointer; }
    input { flex:1; padding:6px 10px; border:1px solid #333; background:#141414; color:#eee; border-radius:6px; outline:none; }
    </style>
    </head>
    <body>
        <div id='bar'>
            <button id='back'>&larr;</button>
            <button id='forward'>&rarr;</button>
            <input id='url' type='text' placeholder='Enter URL' />
            <button id='go'>Go</button>
            <button id='new'>+ Tab</button>
        </div>
        <script>
            function send(obj){ window.ipc.postMessage(JSON.stringify(obj)); }
            document.getElementById('go').onclick = ()=> send({type:'navigate', url: document.getElementById('url').value});
            document.getElementById('new').onclick = ()=> send({type:'new_tab'});
            document.getElementById('url').addEventListener('keydown', e=>{ if(e.key==='Enter') send({type:'navigate', url: document.getElementById('url').value}); });
            window.host_update = function(payloadJson){
                try{ const payload = JSON.parse(payloadJson); const tabs = payload.tabs || []; const active = payload.active;
                    const urlInput = document.getElementById('url');
                    const activeTab = tabs.find(x=>x.id===active);
                    if(activeTab) urlInput.value = activeTab.url || '';
                }catch(e){}
            }
        </script>
    </body>
    </html>"####.to_string();

    use serde_json::Value;
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::rc::Rc;
    use wry::WebView;
    let tabs: Rc<RefCell<HashMap<u64, WebView>>> = Rc::new(RefCell::new(HashMap::new()));
    let active_tab: Rc<RefCell<Option<u64>>> = Rc::new(RefCell::new(None));
    let next_tab_id: Rc<RefCell<u64>> = Rc::new(RefCell::new(1));
    let toolbar_handle: Rc<RefCell<Option<WebView>>> = Rc::new(RefCell::new(None));
    let pending_cmds: Rc<RefCell<Vec<Value>>> = Rc::new(RefCell::new(Vec::new()));
    let pending_for_ipc = pending_cmds.clone();
    let toolbar_ipc = move |req: wry::http::Request<String>| {
        let msg = req.body().clone();
        if let Ok(v) = serde_json::from_str::<Value>(&msg) {
            pending_for_ipc.borrow_mut().push(v);
        }
    };
    let toolbar_height: f64 = 48.0;
    let win_size = window.inner_size();
    let win_w = win_size.width as f64;
    let win_h = win_size.height as f64;
    let toolbar_bounds = Rect {
        x: 0_i32,
        y: 0_i32,
        width: win_w as u32,
        height: toolbar_height as u32,
    };
    let content_bounds = Rect {
        x: 0_i32,
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
    let toolbar_clone_for_handler = toolbar_handle.clone();
    let tabs_for_handler = tabs.clone();
    let active_clone = active_tab.clone();
    let initial_wv = WebViewBuilder::new_as_child(&window)
        .with_bounds(content_bounds)
        .with_url(&initial_url)
        .with_on_page_load_handler(move |_evt, _url| {
            if let Some(tb) = &*toolbar_clone_for_handler.borrow() {
                let mut list = vec![];
                for (id, wv) in tabs_for_handler.borrow().iter() {
                    let u = wv.url().unwrap_or_else(|_| "".to_string());
                    list.push(serde_json::json!({"id": id, "url": u}));
                }
                let payload = serde_json::json!({"tabs": list, "active": *active_clone.borrow()});
                let _ =
                    tb.evaluate_script(&format!("window.host_update('{}')", payload.to_string()));
            }
        })
        .build()?;
    let _ = initial_wv.set_visible(true);
    tabs.borrow_mut().insert(initial_id, initial_wv);
    *active_tab.borrow_mut() = Some(initial_id);
    let toolbar_webview = WebViewBuilder::new_as_child(&window)
        .with_bounds(toolbar_bounds)
        .with_html(&toolbar_html)
        .with_ipc_handler(toolbar_ipc)
        .build()?;
    toolbar_handle.borrow_mut().replace(toolbar_webview);
    if let Some(tb) = &*toolbar_handle.borrow() {
        let _ = tb.set_bounds(toolbar_bounds);
        let _ = tb.set_visible(true);
    }
    event_loop.run(move |event, _, control_flow| {
        *control_flow = ControlFlow::Wait;
        {
            let mut cmds_ref = pending_cmds.borrow_mut();
            if !cmds_ref.is_empty() {
                let items: Vec<_> = cmds_ref.drain(..).collect();
                drop(cmds_ref);
                for v in items {
                    if let Some(t) = v.get("type").and_then(|s| s.as_str()) {
                        match t {
                            "new_tab" => {
                                let id = { let mut n = next_tab_id.borrow_mut(); let v = *n; *n += 1; v };
                                let tb_clone = toolbar_handle.clone();
                                let active_clone = active_tab.clone();
                                let tabs_clone2 = tabs.clone();
                                let tabs_for_handler = tabs.clone();
                                match WebViewBuilder::new_as_child(&window)
                                    .with_url(&initial_url)
                                    .with_on_page_load_handler(move |_evt, _url| {
                                        if let Some(tb) = &*tb_clone.borrow() {
                                            let mut list = vec![];
                                            for (id, wv) in tabs_for_handler.borrow().iter() {
                                                let u = wv.url().unwrap_or_else(|_| "".to_string());
                                                list.push(serde_json::json!({"id": id, "url": u}));
                                            }
                                            let payload = serde_json::json!({"tabs": list, "active": *active_clone.borrow()});
                                            let _ = tb.evaluate_script(&format!("window.host_update('{}')", payload.to_string()));
                                        }
                                    })
                                    .build()
                                {
                                    Ok(wv) => {
                                            for (_other_id, other_wv) in tabs_clone2.borrow_mut().iter_mut() {
                                                let _ = other_wv.set_visible(false);
                                            }
                                            let _ = wv.set_visible(true);
                                            tabs_clone2.borrow_mut().insert(id, wv);
                                            *active_tab.borrow_mut() = Some(id);
                                            if let Some(tb) = &*toolbar_handle.borrow() {
                                                let mut list = vec![];
                                                for (id, wv) in tabs_clone2.borrow().iter() {
                                                    let url = wv.url().unwrap_or_else(|_| "".to_string());
                                                    list.push(serde_json::json!({"id": id, "url": url}));
                                                }
                                                let payload = serde_json::json!({"tabs": list, "active": *active_tab.borrow()});
                                                let _ = tb.evaluate_script(&format!("window.host_update('{}')", payload.to_string()));
                                            }
                                    }
                                    Err(e) => eprintln!("failed to create child webview: {:?}", e),
                                }
                            }
                            "navigate" => {
                                if let Some(url) = v.get("url").and_then(|s| s.as_str()) {
                                    if let Some(active) = *active_tab.borrow() {
                                        if let Some(wv) = tabs.borrow_mut().get_mut(&active) {
                                            let _ = wv.load_url(url);
                                        }
                                    }
                                }
                            }
                            "switch_tab" => {
                                if let Some(tab_id) = v.get("tabId").and_then(|n| n.as_u64()) {
                                    for (id, wv) in tabs.borrow_mut().iter_mut() {
                                        if *id == tab_id { let _ = wv.set_visible(true); } else { let _ = wv.set_visible(false); }
                                    }
                                    *active_tab.borrow_mut() = Some(tab_id);
                                    // update toolbar
                                    if let Some(tb) = &*toolbar_handle.borrow() {
                                        let mut list = vec![];
                                        for (id, wv) in tabs.borrow().iter() {
                                            let url = wv.url().unwrap_or_else(|_| "".to_string());
                                            list.push(serde_json::json!({"id": id, "url": url}));
                                        }
                                        let payload = serde_json::json!({"tabs": list, "active": *active_tab.borrow()});
                                        let _ = tb.evaluate_script(&format!("window.host_update('{}')", payload.to_string()));
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
        }

        match event {
            Event::WindowEvent { event: WindowEvent::Resized(new_size), .. } => {
                // recompute bounds and resize toolbar and child webviews
                let toolbar_height: i32 = 48;
                let win_w = new_size.width as u32;
                let win_h = new_size.height as u32;
                let toolbar_bounds = Rect { x: 0_i32, y: 0_i32, width: win_w, height: toolbar_height as u32 };
                let content_bounds = Rect { x: 0_i32, y: toolbar_height as i32, width: win_w, height: win_h.saturating_sub(toolbar_height as u32) };
                for (_id, wv) in tabs.borrow_mut().iter_mut() {
                    let _ = wv.set_bounds(content_bounds);
                }
                if let Some(tb) = &*toolbar_handle.borrow() {
                    let _ = tb.set_bounds(toolbar_bounds);
                }
            }
            Event::WindowEvent { event: WindowEvent::ScaleFactorChanged { new_inner_size, .. }, .. } => {
                let toolbar_height: i32 = 48;
                let win_w = new_inner_size.width as u32;
                let win_h = new_inner_size.height as u32;
                let toolbar_bounds = Rect { x: 0_i32, y: 0_i32, width: win_w, height: toolbar_height as u32 };
                let content_bounds = Rect { x: 0_i32, y: toolbar_height as i32, width: win_w, height: win_h.saturating_sub(toolbar_height as u32) };
                for (_id, wv) in tabs.borrow_mut().iter_mut() {
                    let _ = wv.set_bounds(content_bounds);
                }
                if let Some(tb) = &*toolbar_handle.borrow() {
                    let _ = tb.set_bounds(toolbar_bounds);
                }
            }
            Event::WindowEvent {
                event: WindowEvent::CloseRequested,
                ..
            } => {
                let _ = backend.kill();
                *control_flow = ControlFlow::Exit;
            }
            _ => {}
        }
    });
}
