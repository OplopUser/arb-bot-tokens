use std::env;
#[cfg(not(test))]
use std::{
    sync::{Mutex, OnceLock},
    time::{Duration, Instant},
};

pub fn env_value(name: &str) -> Option<String> {
    refresh_dotenv_override_throttled();
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

pub fn env_bool(name: &str) -> Option<bool> {
    env_value(name).map(|value| value == "true" || value == "1")
}

pub fn refresh_dotenv_override_throttled() {
    refresh_dotenv_override_throttled_impl();
}

#[cfg(test)]
fn refresh_dotenv_override_throttled_impl() {}

#[cfg(not(test))]
fn refresh_dotenv_override_throttled_impl() {
    let mut last_refresh = dotenv_refresh_state()
        .lock()
        .expect("dotenv refresh lock poisoned");
    let now = Instant::now();
    if last_refresh
        .map(|last| now.duration_since(last) < dotenv_refresh_interval())
        .unwrap_or(false)
    {
        return;
    }
    if preserve_process_env_on_dotenv_refresh() {
        let _ = dotenvy::dotenv();
    } else {
        let _ = dotenvy::dotenv_override();
    }
    *last_refresh = Some(now);
}

#[cfg(not(test))]
fn preserve_process_env_on_dotenv_refresh() -> bool {
    preserve_process_env_on_dotenv_refresh_from_value(
        env::var("ARB_PRESERVE_PROCESS_ENV_ON_DOTENV_REFRESH")
            .ok()
            .as_deref(),
    )
}

fn preserve_process_env_on_dotenv_refresh_from_value(value: Option<&str>) -> bool {
    matches!(
        value.map(str::trim).map(str::to_ascii_lowercase).as_deref(),
        Some("true" | "1" | "yes" | "on")
    )
}

#[cfg(not(test))]
fn dotenv_refresh_interval() -> Duration {
    let millis = env::var("DOTENV_REFRESH_INTERVAL_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(5_000);
    Duration::from_millis(millis)
}

#[cfg(not(test))]
fn dotenv_refresh_state() -> &'static Mutex<Option<Instant>> {
    static STATE: OnceLock<Mutex<Option<Instant>>> = OnceLock::new();
    STATE.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserve_process_env_lock_accepts_boolish_true_values() {
        assert!(preserve_process_env_on_dotenv_refresh_from_value(Some("true")));
        assert!(preserve_process_env_on_dotenv_refresh_from_value(Some("1")));
        assert!(preserve_process_env_on_dotenv_refresh_from_value(Some(" TRUE ")));
    }

    #[test]
    fn preserve_process_env_lock_is_off_by_default() {
        assert!(!preserve_process_env_on_dotenv_refresh_from_value(None));
        assert!(!preserve_process_env_on_dotenv_refresh_from_value(Some("false")));
        assert!(!preserve_process_env_on_dotenv_refresh_from_value(Some("0")));
    }
}
