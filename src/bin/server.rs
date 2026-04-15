use bytes::Bytes;
use mini_redis::{Connection, Frame};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::net::{TcpListener, TcpStream};

type ShardedDb = Arc<Vec<Mutex<HashMap<String, Bytes>>>>;
fn new_sharded_db(num_shards: usize) -> ShardedDb {
    let mut db = Vec::with_capacity(num_shards);
    for _ in 0..num_shards {
        db.push(Mutex::new(HashMap::new()));
    }
    Arc::new(db)
}

fn hash<T: Hash>(t: &T) -> usize {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish() as usize
}

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    println!("Listening!");

    let db = new_sharded_db(8);

    loop {
        let (socket, _) = listener.accept().await.unwrap();
        let db = db.clone();
        tokio::spawn(async move {
            process(socket, db).await;
        });
    }
}

async fn process(socket: TcpStream, db: ShardedDb) {
    use mini_redis::Command::{self, Get, Set};

    let mut connection = Connection::new(socket);

    while let Some(frame) = connection.read_frame().await.unwrap() {
        let response = match Command::from_frame(frame).unwrap() {
            Set(cmd) => {
                let shard_idx = hash(&cmd.key().to_string()) % db.len();
                {
                    let mut shard = db[shard_idx].lock().unwrap();
                    shard.insert(cmd.key().to_string(), cmd.value().clone());
                }
                Frame::Simple("OK".to_string())
            }
            Get(cmd) => {
                let shard_idx = hash(&cmd.key().to_string()) % db.len();
                let result = {
                    let shard = db[shard_idx].lock().unwrap();
                    shard.get(cmd.key()).cloned()
                };

                match result {
                    Some(value) => Frame::Bulk(value),
                    None => Frame::Null,
                }
            }
            cmd => panic!("unimplemented {:?}", cmd),
        };

        connection.write_frame(&response).await.unwrap();
    }
}
