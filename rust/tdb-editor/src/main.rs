#![cfg_attr(windows, windows_subsystem = "windows")]

use std::path::{Path, PathBuf};

use eframe::egui;
use galleria::db::format::Record;
use galleria::ThumbDb;

#[derive(Clone)]
struct Entry {
    key: String,
    value: String,
}

struct TdbEditor {
    path_input: String,
    db: Option<ThumbDb>,
    entries: Vec<Entry>,
    filter: String,
    selected: Option<String>,
    record: Option<Record>,
    key_edit: String,
    value_edit: String,
    new_key: String,
    new_value: String,
    delete_armed: bool,
    thumb_key: Option<String>,
    thumb_small: Option<egui::TextureHandle>,
    thumb_large: Option<egui::TextureHandle>,
    thumb_small_label: String,
    thumb_large_label: String,
    status: String,
    error: Option<String>,
}

impl TdbEditor {
    fn new(initial_path: Option<String>) -> Self {
        let mut app = Self {
            path_input: String::new(),
            db: None,
            entries: Vec::new(),
            filter: String::new(),
            selected: None,
            record: None,
            key_edit: String::new(),
            value_edit: String::new(),
            new_key: String::new(),
            new_value: String::new(),
            delete_armed: false,
            thumb_key: None,
            thumb_small: None,
            thumb_large: None,
            thumb_small_label: String::new(),
            thumb_large_label: String::new(),
            status: "Open a .tdb file to begin. Close the gallery server before editing."
                .to_string(),
            error: None,
        };
        if let Some(path) = initial_path {
            app.open_path(&path);
        }
        app
    }

    fn ok(&mut self, message: impl Into<String>) {
        self.status = message.into();
        self.error = None;
    }

    fn fail(&mut self, message: impl Into<String>) {
        self.error = Some(message.into());
    }

    fn open_path(&mut self, raw: &str) {
        let Some(path) = resolve_db_path(raw) else {
            self.fail("Enter a path to a .tdb file.");
            return;
        };
        if !Path::new(&path).is_file() {
            self.fail(format!("Not a file: {path}"));
            return;
        }
        match ThumbDb::open(&path) {
            Ok(db) => {
                self.path_input = path.clone();
                self.db = Some(db);
                self.selected = None;
                self.record = None;
                self.key_edit.clear();
                self.value_edit.clear();
                self.reload();
                self.ok(format!("Opened {path}"));
            }
            Err(error) => self.fail(format!("Failed to open {path}: {error}")),
        }
    }

    fn create_db(&mut self, raw: &str) {
        let Some(path) = resolve_db_path(raw) else {
            self.fail("Enter a path for the new .tdb file.");
            return;
        };
        if Path::new(&path).exists() {
            self.fail(format!("Refusing to overwrite existing file: {path}"));
            return;
        }
        match ThumbDb::create(&path) {
            Ok(db) => {
                self.path_input = path.clone();
                self.db = Some(db);
                self.selected = None;
                self.record = None;
                self.reload();
                self.ok(format!("Created {path}"));
            }
            Err(error) => self.fail(format!("Failed to create {path}: {error}")),
        }
    }

    fn reload(&mut self) {
        let entries = match &self.db {
            Some(db) => db.iterate(),
            None => Vec::new(),
        };
        let selected_still_present = match (&self.db, &self.selected) {
            (Some(db), Some(key)) => db.get(key).is_some(),
            _ => false,
        };

        let mut entries = entries;
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        self.entries = entries
            .into_iter()
            .map(|(key, value)| Entry { key, value })
            .collect();

        if selected_still_present {
            if let Some(key) = self.selected.clone() {
                self.load_selected(&key);
            }
        } else {
            self.selected = None;
            self.record = None;
            self.key_edit.clear();
            self.value_edit.clear();
            self.delete_armed = false;
        }

        self.status = format!("{} record(s)", self.entries.len());
    }

    fn load_selected(&mut self, key: &str) {
        let record = self.db.as_ref().and_then(|db| db.get_record(key));
        let value = self
            .db
            .as_ref()
            .and_then(|db| db.get(key))
            .unwrap_or_default();
        self.selected = Some(key.to_string());
        self.key_edit = key.to_string();
        self.value_edit = value;
        self.record = record;
        self.delete_armed = false;
        self.thumb_key = None;
        self.thumb_small = None;
        self.thumb_large = None;
        self.thumb_small_label.clear();
        self.thumb_large_label.clear();
    }

    fn save_value(&mut self) {
        let Some(key) = self.selected.clone() else {
            self.fail("Select a record first.");
            return;
        };
        let result = self.db.as_ref().map(|db| db.set(&key, &self.value_edit));
        match result {
            Some(Ok(())) => {
                self.reload();
                self.load_selected(&key);
                self.ok("Value saved.");
            }
            Some(Err(error)) => self.fail(format!("Save failed: {error}")),
            None => self.fail("No database is open."),
        }
    }

    fn rename_key(&mut self) {
        let Some(old_key) = self.selected.clone() else {
            self.fail("Select a record first.");
            return;
        };
        let new_key = self.key_edit.trim().to_string();
        if new_key.is_empty() {
            self.fail("New key cannot be empty.");
            return;
        }
        if new_key == old_key {
            self.ok("Key unchanged.");
            return;
        }
        let exists = self
            .db
            .as_ref()
            .map(|db| db.get(&new_key).is_some())
            .unwrap_or(false);
        if exists {
            self.fail(format!("Key already exists: {new_key}"));
            return;
        }
        let set_result = self
            .db
            .as_ref()
            .map(|db| db.set(&new_key, &self.value_edit));
        match set_result {
            Some(Ok(())) => {
                let delete_result = self.db.as_ref().map(|db| db.delete(&old_key));
                match delete_result {
                    Some(Ok(_)) => {
                        self.reload();
                        self.load_selected(&new_key);
                        self.ok(format!("Renamed {old_key} -> {new_key}"));
                    }
                    Some(Err(error)) => self.fail(format!(
                        "Copied to {new_key} but failed to delete {old_key}: {error}"
                    )),
                    None => self.fail("No database is open."),
                }
            }
            Some(Err(error)) => self.fail(format!("Rename failed: {error}")),
            None => self.fail("No database is open."),
        }
    }

    fn delete_selected(&mut self) {
        let Some(key) = self.selected.clone() else {
            self.delete_armed = false;
            self.fail("Select a record first.");
            return;
        };
        let result = self.db.as_ref().map(|db| db.delete(&key));
        match result {
            Some(Ok(existed)) => {
                self.selected = None;
                self.record = None;
                self.delete_armed = false;
                self.reload();
                self.ok(if existed {
                    format!("Deleted {key}")
                } else {
                    format!("Key was already absent: {key}")
                });
            }
            Some(Err(error)) => self.fail(format!("Delete failed: {error}")),
            None => self.fail("No database is open."),
        }
    }

    fn insert_record(&mut self) {
        let key = self.new_key.trim().to_string();
        let value = self.new_value.trim().to_string();
        if key.is_empty() {
            self.fail("New key cannot be empty.");
            return;
        }
        let exists = self
            .db
            .as_ref()
            .map(|db| db.get(&key).is_some())
            .unwrap_or(false);
        if exists {
            self.fail(format!("Key already exists: {key}"));
            return;
        }
        let result = self.db.as_ref().map(|db| db.set(&key, &value));
        match result {
            Some(Ok(())) => {
                self.new_key.clear();
                self.new_value.clear();
                self.reload();
                self.load_selected(&key);
                self.ok(format!("Inserted {key}"));
            }
            Some(Err(error)) => self.fail(format!("Insert failed: {error}")),
            None => self.fail("No database is open."),
        }
    }

    fn validate(&mut self) {
        match &self.db {
            Some(db) => {
                let code = db.validate();
                self.ok(format!("Validate returned {code}"));
            }
            None => self.fail("No database is open."),
        }
    }

    fn compact(&mut self) {
        let result = self.db.as_ref().map(|db| db.compact());
        match result {
            Some(Ok(())) => {
                self.reload();
                self.ok("Compacted database.");
            }
            Some(Err(error)) => self.fail(format!("Compact failed: {error}")),
            None => self.fail("No database is open."),
        }
    }

    fn sync(&mut self) {
        let result = self.db.as_ref().map(|db| db.sync());
        match result {
            Some(Ok(())) => self.ok("Synced database."),
            Some(Err(error)) => self.fail(format!("Sync failed: {error}")),
            None => self.fail("No database is open."),
        }
    }

    fn verify(&mut self) {
        let result = self.db.as_ref().map(|db| db.verify_records());
        match result {
            Some(Ok((total, valid, missing))) => self.ok(format!(
                "Verified {total} record(s): {valid} with thumbs, {missing} missing thumbs"
            )),
            Some(Err(error)) => self.fail(format!("Verify failed: {error}")),
            None => self.fail("No database is open."),
        }
    }

    fn top_bar(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.heading("TDB Viewer / Editor");
            ui.separator();
            ui.label("DB:");
            let response = ui.add(
                egui::TextEdit::singleline(&mut self.path_input)
                    .desired_width(460.0)
                    .hint_text("path/to/thumbs.tdb"),
            );
            let enter = response.has_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter));
            if ui.button("Open").clicked() || enter {
                let path = self.path_input.clone();
                self.open_path(&path);
            }
            if ui.button("Browse...").clicked() {
                if let Some(path) = rfd::FileDialog::new()
                    .add_filter("TDB database", &["tdb"])
                    .pick_file()
                {
                    let path = path.display().to_string();
                    self.path_input = path.clone();
                    self.open_path(&path);
                }
            }
            if ui.button("New").clicked() {
                let path = self.path_input.clone();
                self.create_db(&path);
            }
            if self.db.is_some() && ui.button("Reload").clicked() {
                self.reload();
            }
        });

        ui.horizontal(|ui| {
            if let Some(db) = &self.db {
                ui.label(
                    egui::RichText::new(db.db_path().display().to_string())
                        .monospace()
                        .small(),
                );
                ui.separator();
                if ui.button("Validate").clicked() {
                    self.validate();
                }
                if ui.button("Verify").clicked() {
                    self.verify();
                }
                if ui.button("Compact").clicked() {
                    self.compact();
                }
                if ui.button("Sync").clicked() {
                    self.sync();
                }
            } else {
                ui.weak("No database open.");
            }
        });

        if let Some(error) = &self.error {
            ui.colored_label(egui::Color32::LIGHT_RED, error);
        } else {
            ui.label(&self.status);
        }
    }

    fn record_list(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.label("Filter:");
            ui.add(
                egui::TextEdit::singleline(&mut self.filter)
                    .desired_width(f32::INFINITY)
                    .hint_text("key or media path"),
            );
        });
        ui.separator();

        let filter = self.filter.trim().to_lowercase();
        let mut clicked = None;
        egui::ScrollArea::vertical()
            .auto_shrink([false, false])
            .show(ui, |ui| {
                let mut shown = 0usize;
                for entry in &self.entries {
                    if !filter.is_empty()
                        && !entry.key.to_lowercase().contains(&filter)
                        && !entry.value.to_lowercase().contains(&filter)
                    {
                        continue;
                    }
                    shown += 1;
                    let selected = self.selected.as_deref() == Some(entry.key.as_str());
                    let text = egui::RichText::new(format!("{}\n{}", entry.key, entry.value))
                        .monospace()
                        .small();
                    if ui.add(egui::Button::selectable(selected, text)).clicked() {
                        clicked = Some(entry.key.clone());
                    }
                }
                if shown == 0 {
                    ui.weak(if self.entries.is_empty() {
                        "No records."
                    } else {
                        "No matching records."
                    });
                }
            });

        if let Some(key) = clicked {
            self.load_selected(&key);
        }
    }

    fn show_thumbnail(&mut self, ui: &mut egui::Ui) {
        let Some(key) = self.selected.clone() else {
            return;
        };
        let parent = self
            .db
            .as_ref()
            .and_then(|db| db.db_path().parent().map(|parent| parent.to_path_buf()));

        if self.thumb_key.as_deref() != Some(key.as_str()) {
            self.thumb_key = Some(key.clone());
            self.thumb_small = None;
            self.thumb_large = None;
            self.thumb_small_label.clear();
            self.thumb_large_label.clear();
            if let Some(parent) = parent {
                let small_path = parent.join(format!("{key}-small.jpg"));
                let large_path = parent.join(format!("{key}-large.jpg"));
                let (small, small_label) = load_thumb(ui, &small_path, &key, "small");
                let (large, large_label) = load_thumb(ui, &large_path, &key, "large");
                self.thumb_small = small;
                self.thumb_large = large;
                self.thumb_small_label = small_label;
                self.thumb_large_label = large_label;
            }
        }

        let max_width = ((ui.available_width() - 24.0) / 2.0).max(180.0);
        ui.horizontal_wrapped(|ui| {
            show_thumb_slot(
                ui,
                "Small",
                self.thumb_small.as_ref(),
                &self.thumb_small_label,
                max_width,
            );
            show_thumb_slot(
                ui,
                "Large",
                self.thumb_large.as_ref(),
                &self.thumb_large_label,
                max_width,
            );
        });
        ui.separator();
    }

    fn details(&mut self, ui: &mut egui::Ui) {
        self.show_thumbnail(ui);
        let record = self.record.clone();
        if let Some(record) = &record {
            ui.heading("Record detail");
            egui::Grid::new("record_detail_grid")
                .num_columns(2)
                .spacing([16.0, 4.0])
                .striped(true)
                .show(ui, |ui| {
                    kv(ui, "Key", &record.filename);
                    kv(ui, "Sequence", &record.sequence.to_string());
                    kv(
                        ui,
                        "Timestamp",
                        &format!("{} ({})", fmt_timestamp(record.timestamp), record.timestamp),
                    );
                    kv(ui, "Media type", record.meta.media_type.name());
                    kv(ui, "Animated", if record.meta.animated { "yes" } else { "no" });
                    kv(ui, "Thumb mode", &record.meta.thumb_mode.to_string());
                    kv(ui, "Hash mode", record.meta.hash_mode.name());
                    kv(ui, "Hash", &hex(&record.hash));
                    kv(
                        ui,
                        "Dimensions",
                        &format!("{} x {}", record.width, record.height),
                    );
                    kv(ui, "Duration", &format!("{} s", record.duration));
                    kv(ui, "CRC32", &format!("{:08x}", record.crc32));
                    kv(ui, "Orientation", &record.orientation.to_string());
                    let perceptual = if record.perceptual_hash_valid {
                        hex(&record.perceptual_hash)
                    } else {
                        "n/a".to_string()
                    };
                    kv(ui, "Perceptual hash", &perceptual);
                    kv(
                        ui,
                        "Codec",
                        record.codec_info.as_deref().unwrap_or("n/a"),
                    );
                    kv(
                        ui,
                        "Directories",
                        &format!(
                            "{} [{}]",
                            record.dir_count,
                            record
                                .dir_indexes
                                .iter()
                                .map(|index| index.to_string())
                                .collect::<Vec<_>>()
                                .join(", ")
                        ),
                    );
                    kv(
                        ui,
                        "GPS",
                        &format!("{:.6}, {:.6}", record.gps_lat, record.gps_lon),
                    );
                });
        } else {
            ui.label("Select a record to inspect it.");
        }

        ui.separator();
        ui.heading("Edit selected record");
        ui.add_enabled_ui(self.selected.is_some(), |ui| {
            ui.horizontal(|ui| {
                ui.label("Key:");
                ui.add(
                    egui::TextEdit::singleline(&mut self.key_edit).desired_width(360.0),
                );
                if ui.button("Rename").clicked() {
                    self.rename_key();
                }
            });
            ui.label(
                egui::RichText::new("Value (media path, or legacy small;large;media)")
                    .small()
                    .weak(),
            );
            ui.add(
                egui::TextEdit::multiline(&mut self.value_edit)
                    .desired_rows(3)
                    .desired_width(f32::INFINITY)
                    .font(egui::TextStyle::Monospace),
            );
            ui.horizontal(|ui| {
                if ui.button("Save value").clicked() {
                    self.save_value();
                }
                if self.delete_armed {
                    if ui
                        .button(egui::RichText::new("Confirm delete").color(egui::Color32::LIGHT_RED))
                        .clicked()
                    {
                        self.delete_selected();
                    }
                    if ui.button("Cancel").clicked() {
                        self.delete_armed = false;
                    }
                } else if ui.button("Delete record").clicked() {
                    self.delete_armed = true;
                }
            });
        });

        ui.separator();
        ui.heading("Insert record");
        ui.horizontal(|ui| {
            ui.label("Key:");
            ui.add(egui::TextEdit::singleline(&mut self.new_key).desired_width(260.0));
        });
        ui.add(
            egui::TextEdit::multiline(&mut self.new_value)
                .desired_rows(2)
                .desired_width(f32::INFINITY)
                .hint_text("media path")
                .font(egui::TextStyle::Monospace),
        );
        if ui.button("Insert").clicked() {
            self.insert_record();
        }
    }
}

impl eframe::App for TdbEditor {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        let dropped = ctx.input(|input| {
            input
                .raw
                .dropped_files
                .iter()
                .filter_map(|file| file.path.clone())
                .next()
        });
        if let Some(path) = dropped {
            let path = path.display().to_string();
            self.path_input = path.clone();
            self.open_path(&path);
        }

        egui::TopBottomPanel::top("top_bar").show(ctx, |ui| {
            self.top_bar(ui);
        });

        egui::SidePanel::left("record_list")
            .resizable(true)
            .default_width(440.0)
            .show(ctx, |ui| {
                self.record_list(ui);
            });

        egui::CentralPanel::default().show(ctx, |ui| {
            egui::ScrollArea::vertical()
                .auto_shrink([false, false])
                .show(ui, |ui| {
                    self.details(ui);
                });
        });
    }
}

fn load_thumb(
    ui: &egui::Ui,
    path: &Path,
    key: &str,
    tag: &str,
) -> (Option<egui::TextureHandle>, String) {
    if !path.is_file() {
        return (None, format!("{}: not found", path.display()));
    }
    let image = match std::fs::read(path) {
        Ok(bytes) => match image::load_from_memory(&bytes) {
            Ok(image) => image,
            Err(error) => return (None, format!("{}: {error}", path.display())),
        },
        Err(error) => return (None, format!("{}: {error}", path.display())),
    };
    let rgba = image.to_rgba8();
    let size = [rgba.width() as usize, rgba.height() as usize];
    let color = egui::ColorImage::from_rgba_unmultiplied(size, rgba.as_raw());
    let texture = ui.ctx().load_texture(
        format!("thumb-{key}-{tag}"),
        color,
        egui::TextureOptions::LINEAR,
    );
    (Some(texture), path.display().to_string())
}

fn show_thumb_slot(
    ui: &mut egui::Ui,
    title: &str,
    texture: Option<&egui::TextureHandle>,
    label: &str,
    max_width: f32,
) {
    ui.vertical(|ui| {
        ui.strong(title);
        if let Some(texture) = texture {
            let original = texture.size_vec2();
            let max_height = 380.0;
            let scale = (max_width / original.x)
                .min(max_height / original.y)
                .min(1.0);
            let size = original * scale;
            ui.add(egui::Image::new((texture.id(), size)));
        }
        ui.label(egui::RichText::new(label).small().weak());
    });
}

fn resolve_db_path(raw: &str) -> Option<String> {
    let trimmed = raw.trim().trim_matches('"').trim_matches('\'').trim();
    if trimmed.is_empty() {
        return None;
    }
    let path = PathBuf::from(trimmed);
    if path.is_dir() {
        Some(path.join("thumbs.tdb").display().to_string())
    } else {
        Some(trimmed.to_string())
    }
}

fn kv(ui: &mut egui::Ui, key: &str, value: &str) {
    ui.label(key);
    ui.monospace(value);
    ui.end_row();
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn fmt_timestamp(seconds: u64) -> String {
    let days = (seconds / 86_400) as i64;
    let remainder = seconds % 86_400;
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02} UTC",
        remainder / 3_600,
        (remainder % 3_600) / 60,
        remainder % 60
    )
}

fn civil_from_days(days_since_epoch: i64) -> (i64, u32, u32) {
    let z = days_since_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if m <= 2 { y + 1 } else { y };
    (year, m as u32, d as u32)
}

fn main() -> eframe::Result<()> {
    let initial_path = std::env::args().nth(1);
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1280.0, 860.0])
            .with_min_inner_size([800.0, 520.0])
            .with_maximized(true),
        ..Default::default()
    };
    eframe::run_native(
        "TDB Viewer / Editor",
        options,
        Box::new(move |cc| {
            cc.egui_ctx.set_visuals(egui::Visuals::dark());
            Ok(Box::new(TdbEditor::new(initial_path)))
        }),
    )
}
