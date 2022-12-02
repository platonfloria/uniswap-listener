use std::{env, time::SystemTime, collections::HashMap};

use ethabi::ethereum_types::U64;
use ethereum_abi::{Abi, Value};
use hex_literal::hex;
use num_bigfloat::BigFloat;
use tokio::sync::{mpsc::{self, Sender, Receiver}, Mutex};
use web3::{
    futures::StreamExt,
    transports::WebSocket,
    types::{FilterBuilder, Address, BlockNumber, BlockId, Log}, contract::{Contract, Options},
    Web3
};


fn convert_q64_96(q64_96: ethereum_types::U256) -> BigFloat {
    let least_sig = q64_96.0[0];
    let second_sig = q64_96.0[1];
    let third_sig = q64_96.0[2];
    let most_sig = q64_96.0[3];

    let bf2 = BigFloat::from(2);
    let bf64 = BigFloat::from(64);
    let bf128 = BigFloat::from(128);
    let bf192 = BigFloat::from(192);
    let bf96 = BigFloat::from(96);

    (
        (BigFloat::from(most_sig) * bf2.pow(&bf192)) +
        (BigFloat::from(third_sig) * bf2.pow(&bf128)) +
        (BigFloat::from(second_sig) * bf2.pow(&bf64)) +
        BigFloat::from(least_sig)
    ) / bf2.pow(&bf96)
}

enum PoolType {
    V2,
    V3,
}

struct Controller {
    web3: web3::Web3<WebSocket>,
    erc20_contract: ethabi::Contract,
    uniswap_v2_contract: ethabi::Contract,
    uniswap_v2_abi: Abi,
    uniswap_v3_contract: ethabi::Contract,
    uniswap_v3_abi: Abi,
    pool_info_cache: Mutex<HashMap<Address, (String, u32, u32)>>,
    erc20_info_cache: Mutex<HashMap<Address, (String, u32)>>,
}

impl Controller {
    pub fn new(web3: web3::Web3<WebSocket>) -> Result<Self, serde_json::Error> {
        Ok(Controller {
            web3,
            erc20_contract: serde_json::from_str(include_str!("../resources/ERC20.abi"))?,
            uniswap_v2_contract: serde_json::from_str(include_str!("../resources/IUniswapV2Pair.abi"))?,
            uniswap_v2_abi: serde_json::from_str(include_str!("../resources/IUniswapV2Pair.abi"))?,
            uniswap_v3_contract: serde_json::from_str(include_str!("../resources/IUniswapV3Pair.abi"))?,
            uniswap_v3_abi: serde_json::from_str(include_str!("../resources/IUniswapV3Pair.abi"))?,
            pool_info_cache: Mutex::new(HashMap::new()),
            erc20_info_cache: Mutex::new(HashMap::new()),
        })
    }

    async fn listen_to_events(&self, from_block_number: U64, log_sender: Sender<(SystemTime, Log)>) -> web3::contract::Result<()> {
        let filter = FilterBuilder::default()
            .from_block(BlockNumber::Number(from_block_number))
            .topics(
                Some(vec![
                    hex!("1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1").into(),
                    hex!("c42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67").into()
                ]),
                None,
                None,
                None,
            )
            .build();

        let sub = self.web3.eth_subscribe().subscribe_logs(filter).await?;

        sub.fold(log_sender, |log_sender, log| async move {
            if let Ok(log) = log {
                let timestamp = SystemTime::now();
                log_sender.send((timestamp, log)).await.expect("failed to push log");
            }

            log_sender
        }).await;

        Ok(())
    }

    async fn process_logs(&self, mut log_receiver: Receiver<(SystemTime, Log)>) -> web3::contract::Result<()> {
        loop {
            if let Some((timestamp, log)) = log_receiver.recv().await {
                match self.process_log(&log).await {
                    Ok((block_number, transaction_hash, pool_contract_address, pool_type, symbol, price)) => {
                        println!(
                            "
Received timestamp: {}
Block number: {}
Transaction hash: 0x{}
Pool contract address: 0x{}
Pool type: {}
Token's pair: {}
New price: {}
                            ",
                            timestamp.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs_f64(),
                            block_number,
                            transaction_hash,
                            pool_contract_address,
                            match pool_type {PoolType::V2 => "V2", PoolType::V3 => "V3"},
                            symbol,
                            price
                        );
                    },
                    Err(_) => {
                        println!("failed to process the log\n{:?}", log);
                    }
                }
            }
        }
    }

    async fn process_log(&self, log: &Log) -> web3::contract::Result<(
        u64,
        String,
        String,
        PoolType,
        String,
        f64,
    )> {
        let block_number = log.block_number.ok_or(web3::Error::InvalidResponse(String::from("log does not contain block number")))?;
        let transaction_hash = log.transaction_hash.ok_or(web3::Error::InvalidResponse(String::from("log does not contain transaction hash")))?;
        let pool_contract_address = log.address;
        let topic = hex::encode(log.topics[0].as_bytes());
        let (pool_type, symbol, price) =  match topic.as_str() {
            "1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1" => {
                let (symbol, decimals0, decimals1) = self.get_uniswap_v2_pool_info(pool_contract_address).await?;

                let topics: Vec<_> = log.topics.iter().map(|&t| ethereum_types::H256::from_slice(&t.to_fixed_bytes())).collect();
                let (_, log_data) = self.uniswap_v2_abi
                    .decode_log_from_slice(&topics, &log.data.0)
                    .expect("failed decoding log");
                let reserve0 = match log_data.reader().by_name.get("reserve0").ok_or(web3::Error::InvalidResponse(String::from("log data does not contain reserve0")))?.value {
                    Value::Uint(value, _) => value.as_u128(),
                    _ => 0
                };
                let reserve1 = match log_data.reader().by_name.get("reserve1").ok_or(web3::Error::InvalidResponse(String::from("log data does not contain reserve1")))?.value {
                    Value::Uint(value, _) => value.as_u128(),
                    _ => 0
                };
                let price = (BigFloat::from(reserve1) / BigFloat::from(reserve0) / BigFloat::from(10).pow(&BigFloat::from(decimals1 as i32 - decimals0 as i32))).to_f64();
                Some((PoolType::V2, symbol, price))
            },
            "c42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67" => {
                let (symbol, decimals0, decimals1) = self.get_uniswap_v3_pool_info(pool_contract_address).await?;

                let topics: Vec<_> = log.topics.iter().map(|&t| ethereum_types::H256::from_slice(&t.to_fixed_bytes())).collect();
                let (_, log_data) = self.uniswap_v3_abi
                    .decode_log_from_slice(&topics, &log.data.0)
                    .expect("failed decoding log");
                let price = match log_data.reader().by_name.get("sqrtPriceX96").ok_or(web3::Error::InvalidResponse(String::from("log data does not contain sqrtPriceX96")))?.value {
                    Value::Uint(value, _) => {
                        (convert_q64_96(value).pow(&BigFloat::from(2)) / BigFloat::from(10).pow(&BigFloat::from(decimals1 as i32 - decimals0 as i32))).to_f64()
                    },
                    _ => 0.0
                };

                Some((PoolType::V3, symbol, price))
            },
            _ => None
        }.unwrap();
        Ok((
            block_number.as_u64(),
            hex::encode(transaction_hash.as_bytes()),
            hex::encode(pool_contract_address.as_bytes()),
            pool_type,
            symbol,
            price
        ))
    }

    async fn get_uniswap_v2_pool_info(&self, address: Address) -> web3::contract::Result<(String, u32, u32)> {
        if let Some((symbol, decimals0, decimals1)) = self.pool_info_cache.lock().await.get(&address) {
            return Ok((symbol.clone(), *decimals0, *decimals1));
        }
        let contract = Contract::new(self.web3.eth(), address, self.uniswap_v2_contract.clone());
        let token0_address: Address = contract.query("token0", (), None, Options::default(), None).await?;
        let (symbol0, decimals0) = self.get_erc20_info(token0_address).await?;
        let token1_address: Address = contract.query("token1", (), None, Options::default(), None).await?;
        let (symbol1, decimals1) = self.get_erc20_info(token1_address).await?;
        let symbol = format!("{}/{}", symbol0, symbol1);
        self.pool_info_cache.lock().await.insert(address, (symbol.clone(), decimals0, decimals1));
        Ok((symbol, decimals0, decimals1))
    }

    async fn get_uniswap_v3_pool_info(&self, address: Address) -> web3::contract::Result<(String, u32, u32)> {
        if let Some((symbol, decimals0, decimals1)) = self.pool_info_cache.lock().await.get(&address) {
            return Ok((symbol.clone(), *decimals0, *decimals1));
        }
        let contract = Contract::new(self.web3.eth(), address, self.uniswap_v3_contract.clone());
        let token0_address: Address = contract.query("token0", (), None, Options::default(), None).await?;
        let (symbol0, decimals0) = self.get_erc20_info(token0_address).await?;
        let token1_address: Address = contract.query("token1", (), None, Options::default(), None).await?;
        let (symbol1, decimals1) = self.get_erc20_info(token1_address).await?;
        let symbol = format!("{}/{}", symbol0, symbol1);
        self.pool_info_cache.lock().await.insert(address, (symbol.clone(), decimals0, decimals1));
        Ok((symbol, decimals0, decimals1))
    }

    async fn get_erc20_info(&self, address: Address) -> web3::contract::Result<(String, u32)> {
        if let Some((symbol, decimals)) = self.erc20_info_cache.lock().await.get(&address) {
            return Ok((symbol.clone(), *decimals))
        }
        let contract = Contract::new(self.web3.eth(), address, self.erc20_contract.clone());
        let symbol: String = contract.query("symbol", (), None, Options::default(), None).await?;
        let decimals: u32 = contract.query("decimals", (), None, Options::default(), None).await?;
        self.erc20_info_cache.lock().await.insert(address, (symbol.clone(), decimals));
        Ok((symbol, decimals))
    }
}


#[tokio::main]
async fn main() -> web3::contract::Result<()> {
    dotenv::dotenv().ok();

    let _ = env_logger::try_init();

    let websocket = WebSocket::new(&env::var("INFURA_MAIN").expect("INFURA_MAIN env variable not set")).await?;
    let web3 = Web3::new(websocket);

    let (log_sender, log_receiver) = mpsc::channel(1);

    let latest_block_number = web3
        .eth()
        .block(BlockId::Number(BlockNumber::Latest))
        .await?
        .ok_or(web3::Error::InvalidResponse(String::from("unable to obtain latest block")))?
        .number
        .ok_or(web3::Error::InvalidResponse(String::from("unable to obtain latest block number")))?;

    let controller = Controller::new(web3).expect("failed to load ABI files");

    match tokio::join!(
        controller.listen_to_events(latest_block_number, log_sender),
        controller.process_logs(log_receiver),
    ) {
        _ => Ok(())
    }
}