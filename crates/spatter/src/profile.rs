//! Opt-in wall-time spans; nested/concurrent spans must not be summed.
pub(crate) struct Span(Option<(&'static str, std::time::Instant)>);

impl Span {
    pub(crate) fn new(stage: &'static str) -> Self {
        Self(
            (std::env::var_os("SPATTER_PROFILE").is_some())
                .then(|| (stage, std::time::Instant::now())),
        )
    }
}

impl Drop for Span {
    fn drop(&mut self) {
        if let Some((stage, start)) = self.0 {
            crate::metrics::log_line(format!(
                "PROFILE pid={} rank={} stage={} us={}",
                std::process::id(),
                std::env::var("SPATTER_RANK").unwrap_or_else(|_| "0".into()),
                stage,
                start.elapsed().as_micros()
            ));
        }
    }
}
