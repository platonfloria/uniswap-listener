# uniswap-listener
Service that listens for sync and swap events from UniswapV2 and UniswapV3 pools and prints out various data to the console.

## configuration
Create an .env file with the following variable:
```
INFURA_MAIN=wss://mainnet.infura.io/ws/v3/<infura_api_key>
```
You can get infura_api_key from here https://app.infura.io.

## build
```bash
cargo build
```

## run
```bash
cargo run
```