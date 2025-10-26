use std::{
    env,
    path::PathBuf,
    process::{Child, Command},
};
use wry::{
    application::{
        dpi::LogicalSize,
        event::{Event, WindowEvent},
        event_loop::{ControlFlow, EventLoop},
        window::WindowBuilder,
    },
    webview::WebViewBuilder,
    Result,
};

fn main() -> Result<()> {
    let backend_exe = if cfg!(target_os = "windows") {
        "galleria.exe"
    } else {
        "./galleria"
    };
    let mut backend: Child = Command::new(backend_exe)
        .spawn()
        .expect("failed to start backend");
    let event_loop = EventLoop::new();
    let window = WindowBuilder::new()
        .with_title("Galleria")
        .with_maximized(true)
        .with_inner_size(LogicalSize::new(800.0, 600.0))
        .build(&event_loop)?;
    let appdata_dir = dirs::data_local_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("Galleria");
    std::fs::create_dir_all(&appdata_dir).expect("Failed to create AppData folder");
    if cfg!(target_os = "windows") {
        env::set_var("WEBVIEW2_USER_DATA_FOLDER", &appdata_dir);
    }
    let _webview = WebViewBuilder::new(window)?
        .with_url("http://localhost:3000/")?
        .build()?;
    event_loop.run(move |event, _, control_flow| {
        *control_flow = ControlFlow::Wait;
        if let Event::WindowEvent { event, .. } = event {
            if let WindowEvent::CloseRequested = event {
                let _ = backend.kill();
                *control_flow = ControlFlow::Exit;
            }
        }
    });
}
