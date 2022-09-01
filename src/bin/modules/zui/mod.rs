use crossterm::event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use std::error::Error;
use std::io;
use std::time::{Duration, Instant};
use tui::backend::{Backend, CrosstermBackend};
use tui::Terminal;

use app::App;

mod app;
mod ui;

pub async fn run() -> Result<(), Box<dyn Error>> {
    // initialize stubs
    let client = rbus::Client::new("redis://0.0.0.0:6379").await.unwrap();

    let tick_rate = Duration::from_millis(250);

    // setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // create app and run it
    let app = App::new(client);
    // spawn poll services
    app.poll_version().await;
    app.poll_reserved_stream().await;
    app.poll_cpu_usage().await;
    app.poll_memory_usage().await;
    app.poll_zos_addresses().await;
    app.poll_dmz_addresses().await;
    app.poll_ygg_addresses().await;
    app.poll_public_addresses().await;
    let res = run_app(&mut terminal, app, tick_rate).await;
    // restore terminal
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    if let Err(err) = res {
        println!("{:?}", err)
    }

    Ok(())
}
async fn run_app<B: Backend>(
    terminal: &mut Terminal<B>,
    mut app: App,
    tick_rate: Duration,
) -> io::Result<()> {
    let mut last_tick = Instant::now();
    loop {
        terminal.draw(|f| ui::draw(f, &mut app))?;

        let timeout = tick_rate
            .checked_sub(last_tick.elapsed())
            .unwrap_or_else(|| Duration::from_secs(0));
        if crossterm::event::poll(timeout)? {
            if let Event::Key(key) = event::read()? {
                match key.code {
                    KeyCode::Char(c) => app.on_key(c),
                    _ => {}
                }
            }
        }
        if last_tick.elapsed() >= tick_rate {
            app.on_tick().await;
            last_tick = Instant::now();
        }
        if app.should_quit {
            return Ok(());
        }
    }
}
