use std::env;
use tui::{
    backend::Backend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Span, Spans},
    widgets::{Block, Borders, Cell, Paragraph, Row, Table, Wrap},
    Frame,
};

use crate::{app::App, zos_traits::Capacity};

pub fn draw<B: Backend>(f: &mut Frame<B>, app: &mut App) {
    let chunks = Layout::default()
        .constraints(
            [
                Constraint::Percentage(20),
                Constraint::Percentage(40),
                Constraint::Percentage(40),
            ]
            .as_ref(),
        )
        .split(f.size());

    draw_header(f, chunks[0], app);
    draw_network(f, chunks[1], app);
    draw_system_capacity(f, chunks[2], app);
}
fn draw_header<B>(f: &mut Frame<B>, area: Rect, app: &mut App)
where
    B: Backend,
{
    let info_style: Style = Style::default().fg(Color::Green);
    let error_style: Style = Style::default().fg(Color::Red);
    let node_id_span = match &app.node_id {
        Ok(node_id) => Span::styled(format!("{}", node_id), info_style),
        Err(err) => Span::styled(format!("{}", err), error_style),
    };
    let farm_id_span = match &app.farm_id {
        Ok(farm_id) => Span::styled(format!("{}", farm_id), info_style),
        Err(err) => Span::styled(format!("{}", err), error_style),
    };
    let farm_name_span = match &app.farm_name {
        Ok(farm_name) => Span::styled(format!("{}", farm_name), info_style),
        Err(err) => Span::styled(format!("{}", err), error_style),
    };

    let text = vec![
        // Spans::from(""),
        // Spans::from(r" ____  _____  ___"),
        // Spans::from(r"(_   )(  _  )/ __)"),
        // Spans::from(r" / /_  )(_)( \__ \"),
        // Spans::from(r"(____)(_____)(___/"),
        Spans::from(vec![
            Span::from("Welcome to "),
            Span::styled("Zero-OS", Style::default().fg(Color::Yellow)),
            Span::raw(", "),
            Span::styled("Threefold", Style::default().fg(Color::Blue)),
            Span::raw(" Autonomous Operating System"),
        ]),
        Spans::from(vec![
            Span::raw("This is node "),
            node_id_span,
            Span::raw(" (farmer "),
            farm_id_span,
            Span::raw(": "),
            farm_name_span,
            Span::raw(")"),
        ]),
        Spans::from(vec![
            Span::raw("Running Zero-OS version"),
            Span::styled(
                format!(" {}", app.version.lock().unwrap()),
                Style::default().fg(Color::Blue),
            ),
        ]),
    ];
    let block = Block::default().borders(Borders::ALL);
    let paragraph = Paragraph::new(text)
        .block(block)
        .wrap(Wrap { trim: true })
        .alignment(tui::layout::Alignment::Center);
    f.render_widget(paragraph, area);
}
fn draw_network<B>(f: &mut Frame<B>, area: Rect, app: &mut App)
where
    B: Backend,
{
    let zos = app
        .zos_addresses
        .lock()
        .unwrap()
        .to_string()
        .trim()
        .to_string();
    let dmz = app
        .dmz_addresses
        .lock()
        .unwrap()
        .to_string()
        .trim()
        .to_string();
    let ygg = app
        .ygg_addresses
        .lock()
        .unwrap()
        .to_string()
        .trim()
        .to_string();
    let public_addresses = app
        .pub_addresses
        .lock()
        .unwrap()
        .to_string()
        .trim()
        .to_string();
    let exit_device = match &app.exit_device {
        Ok(exit_device) => format!("{}", exit_device.to_string()),
        Err(err) => format!("{}", err),
    };
    let rows = vec![
        Row::new(vec!["ZOS", &zos]),
        Row::new(vec!["DMZ", &dmz]).style(Style::default().fg(Color::Blue)),
        Row::new(vec!["YGG", &ygg]),
        Row::new(vec!["PUB", &public_addresses]).style(Style::default().fg(Color::Blue)),
        Row::new(vec!["DUL", &exit_device]),
    ];
    let table = draw_net_table(rows);
    f.render_widget(table, area);
}

fn draw_system_capacity<B>(f: &mut Frame<B>, area: Rect, app: &mut App)
where
    B: Backend,
{
    const GIG: f32 = 1.07374e+09;
    let cru = app.capacity.lock().unwrap().cru.to_string();
    let mru = app.capacity.lock().unwrap().mru as f64 / GIG as f64;
    let mru = format!("{:.0} GB", mru.round());
    let hru = app.capacity.lock().unwrap().hru as f64 / GIG as f64;
    let hru = format!("{:.0} GB", hru.round());
    let sru = app.capacity.lock().unwrap().sru as f64 / GIG as f64;
    let sru = format!("{:.0} GB", sru.round());
    let ipv4 = app.capacity.lock().unwrap().ipv4u.to_string();
    let used_mem_percent = format!("{:.0}%", app.used_mem_percent.lock().unwrap().round());
    let used_cpu_percent = format!("{:.0}%", app.used_cpu_percent.lock().unwrap().round());

    let chunks = Layout::default()
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)].as_ref())
        .direction(Direction::Horizontal)
        .split(area);
    let rows = vec![
        Row::new(vec!["CPU Usage", &used_cpu_percent]),
        Row::new(vec!["CRU Reserved", &cru]).style(Style::default().fg(Color::Blue)),
        Row::new(vec!["SSD Reserved", &sru]),
        Row::new(vec!["IPV4 Reserved", &ipv4]).style(Style::default().fg(Color::Blue)),
    ];
    let table = draw_table(rows);
    f.render_widget(table, chunks[0]);
    let rows = vec![
        Row::new(vec!["Memory Usage", &used_mem_percent]),
        Row::new(vec!["MRU Reserved", &mru]).style(Style::default().fg(Color::Blue)),
        Row::new(vec!["HDD Reserved", &hru]),
    ];
    let table = draw_table(rows);
    f.render_widget(table, chunks[1]);
}

fn draw_table(rows: Vec<Row>) -> Table {
    let t = Table::new(rows)
        .style(Style::default().fg(Color::White))
        .block(
            Block::default()
                .title("System Used Capacity")
                .borders(Borders::ALL),
        )
        .widths(&[Constraint::Percentage(50), Constraint::Percentage(50)])
        .column_spacing(1)
        .highlight_style(Style::default().add_modifier(Modifier::BOLD))
        .highlight_symbol(">>");
    t
}
fn draw_net_table(rows: Vec<Row>) -> Table {
    let t = Table::new(rows)
        .style(Style::default().fg(Color::White))
        .block(
            Block::default()
                .title("System Used Capacity")
                .borders(Borders::ALL),
        )
        .widths(&[Constraint::Percentage(10), Constraint::Percentage(90)])
        .column_spacing(1)
        .highlight_style(Style::default().add_modifier(Modifier::BOLD))
        .highlight_symbol(">>");
    t
}
