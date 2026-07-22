use fastwebsockets::{
  self as fws, after_handshake_split, Frame, OpCode, Role, WebSocketError,
};
use futures_lite::io::Cursor;

fn raw_frame(fin: bool, opcode: OpCode, payload: &[u8]) -> Vec<u8> {
  let mut frame = Vec::new();
  let first_byte = if fin { 0x80 } else { 0x00 } | (opcode as u8);
  frame.push(first_byte);
  let len = payload.len();
  if len < 126 {
    frame.push(len as u8);
  } else if len < 65536 {
    frame.push(126);
    frame.extend_from_slice(&(len as u16).to_be_bytes());
  } else {
    frame.push(127);
    frame.extend_from_slice(&(len as u64).to_be_bytes());
  }
  frame.extend_from_slice(payload);
  frame
}

fn assert_frame_too_large<T>(result: Result<T, WebSocketError>) {
  assert!(matches!(result, Err(WebSocketError::FrameTooLarge)));
}

fn noop_send(
  _: Frame<'_>,
) -> futures_lite::future::Ready<Result<(), WebSocketError>> {
  futures_lite::future::ready(Ok(()))
}

#[test]
fn split_fragment_collector_rejects_aggregate_binary_over_limit() {
  futures_lite::future::block_on(async {
    let frames = vec![
      raw_frame(false, OpCode::Binary, b"12345"),
      raw_frame(true, OpCode::Continuation, b"67890"),
    ];
    let data: Vec<u8> = frames.iter().flatten().copied().collect();

    let (mut ws_read, _ws_write) = after_handshake_split(
      Cursor::new(data),
      futures_lite::io::sink(),
      Role::Client,
    );
    ws_read.set_max_message_size(9);
    let mut ws = fws::FragmentCollectorRead::new(ws_read);

    assert_frame_too_large(ws.read_frame(&mut noop_send).await);
  });
}

#[test]
fn split_fragment_collector_rejects_aggregate_text_over_limit() {
  futures_lite::future::block_on(async {
    let frames = vec![
      raw_frame(false, OpCode::Text, b"hello"),
      raw_frame(true, OpCode::Continuation, b"world"),
    ];
    let data: Vec<u8> = frames.iter().flatten().copied().collect();

    let (mut ws_read, _ws_write) = after_handshake_split(
      Cursor::new(data),
      futures_lite::io::sink(),
      Role::Client,
    );
    ws_read.set_max_message_size(9);
    let mut ws = fws::FragmentCollectorRead::new(ws_read);

    assert_frame_too_large(ws.read_frame(&mut noop_send).await);
  });
}
