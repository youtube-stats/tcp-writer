extern crate chrono;
extern crate postgres;
extern crate quick_protobuf;

pub mod message;
use message::SubMessage;

use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream, TcpListener};
use std::io::Read;
use postgres::{Connection, TlsMode};
use std::process::exit;
use std::thread::spawn;
use quick_protobuf::deserialize_from_slice;
use chrono::{DateTime, Local};
use postgres::transaction::Transaction;
use std::sync::mpsc::{Sender, Receiver, channel};

#[derive(Clone, Debug)]
pub struct ChannelRow {
    pub time: DateTime<Local>,
    pub id: i32,
    pub sub: i32
}

static PORT: u16 = 3335u16;
static SIZE: usize = 1000;
const BUFSIZE: usize = 2000;
static POSTGRESQL_URL: &'static str = "postgresql://admin@localhost:5432/youtube";
static INSERT: &'static str = "INSERT INTO youtube.stats.channels (time, id, serial) VALUE ($1, $2, $3);";

pub fn listen() -> TcpListener {
    let ip: IpAddr = IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0));
    let port: u16 = PORT;
    println!("Listening on port {}", port);

    let addr: SocketAddr = SocketAddr::new(ip, port);
    TcpListener::bind(&addr)
        .expect("unable to bind TCP listener")
}

pub fn write_rows(store: &Vec<ChannelRow>) {
    println!("Writing {} channels", SIZE);
    let conn: Connection = {
        let params: &'static str = POSTGRESQL_URL;
        let tls: TlsMode = TlsMode::None;

        Connection::connect(params, tls)
            .expect("Could not connect to database")
    };

    let query: &'static str = INSERT;
    let trans: Transaction = conn.transaction()
        .expect("Could not create transaction");

    for i in 0..SIZE {
        let row: &ChannelRow = &store[i];
        let time: &DateTime<Local> = &row.time;
        let id: &i32 = &row.id;
        let sub: &i32 = &row.sub;

        conn.execute(query, &[time, id, sub])
            .expect("Could not query db");
    }

    trans.commit()
        .expect("Could not commit transaction");
}

pub fn msg_to_vec(msg: SubMessage) -> Vec<ChannelRow> {
    let mut store: Vec<ChannelRow> = Vec::new();

    let time: DateTime<Local> = Local::now();
    for i in 0..msg.subs.len() {
        let id: i32 = msg.ids[i];
        let sub: i32 = msg.subs[i];

        let value: ChannelRow = ChannelRow {
            time,
            id,
            sub
        };

        store.push(value);
    }

    store
}

fn main() {
    println!("Starting write service");
    let listener: TcpListener = listen();
    let (sx, rx): (Sender<SubMessage>, Receiver<SubMessage>) = channel();

    spawn(move || {
        let mut store: Vec<ChannelRow> = Vec::new();

        loop {
            {
                println!("Waiting for messages");
                let msg = rx.recv()
                    .expect("Could not receive messsage");

                println!("Received message {:?}", msg);
                let mut other: Vec<ChannelRow> = msg_to_vec(msg);
                store.append(&mut other);
            }

            println!("New cache size after insertion {}", store.len());
            if store.len() >= SIZE {
                println!("Write triggered");

                write_rows(&store);
                store.drain(0..SIZE);
                println!("New cache size after write {}", store.len());
            }
        }
    });

    let mut buf: [u8; BUFSIZE] = [0u8; BUFSIZE];
    for stream in listener.incoming() {
        if stream.is_err() {
            eprintln!("Connection is bad: {:?}", stream);
            exit(3);
        }

        let mut stream: TcpStream = stream.unwrap();
        let n_option = stream.read(&mut buf);
        if n_option.is_err() {
            eprintln!("Could not read from socket");
            continue;
        }

        let n: usize = n_option.unwrap();
        println!("Got {} bytes", n);
        let bytes: &[u8] = &buf[0..n];

        let msg_option = deserialize_from_slice(bytes);
        if msg_option.is_err() {
            eprintln!("Could not parse protobuf message");
            continue;
        }

        let t: SubMessage = msg_option.unwrap();
        println!("Meesage was {:?}", t);

        sx.send(t).expect("Could not send message")
    }
}
