use peartube_native::resolution::{VideoSize, resolution_label, video_size};
use serde::Deserialize;
use std::{
    future::Future,
    task::{Context, Poll, Waker},
};

#[derive(Deserialize)]
struct Fixtures {
    media: Vec<Media>,
    labels: Vec<Label>,
}
#[derive(Deserialize)]
struct Media {
    name: String,
    hex: String,
    expected: Option<VideoSize>,
}
#[derive(Deserialize)]
struct Label {
    width: u64,
    height: u64,
    expected: String,
}
fn run<F: Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("fixture reader unexpectedly pending"),
    }
}
#[test]
fn resolution_fixtures() {
    let fixtures: Fixtures =
        serde_json::from_str(include_str!("../fixtures/resolution-reference.json")).unwrap();
    for case in fixtures.media {
        let data: Vec<u8> = case
            .hex
            .as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|b: &[u8; 2]| u8::from_str_radix(std::str::from_utf8(b).unwrap(), 16).unwrap())
            .collect();
        let actual = run(video_size(|offset, len| {
            let start = usize::try_from(offset)
                .unwrap_or(usize::MAX)
                .min(data.len());
            let end = start.saturating_add(len).min(data.len());
            std::future::ready(Ok(data[start..end].to_vec()))
        }))
        .unwrap();
        assert_eq!(actual, case.expected, "{}", case.name);
    }
    for case in fixtures.labels {
        assert_eq!(
            resolution_label(VideoSize {
                width: case.width,
                height: case.height
            }),
            case.expected
        );
    }
}
