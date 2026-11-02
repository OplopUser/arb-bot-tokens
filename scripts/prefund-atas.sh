#!/bin/bash
# Pre-fund ATAs for all seed pair base tokens
# Saves $0.15 ATA rent + 1 instruction per first arb trade on each pair
# Run once: bash scripts/prefund-atas.sh

set -e

WALLET_KEYPAIR="${SOLANA_WALLET_JSON:-.secrets/wallet.json}"
RPC_URL="${HELIOS_RPC_URL:-https://api.mainnet-beta.solana.com}"

echo "=== Pre-funding ATAs for seed pair base tokens ==="
echo "Wallet: $WALLET_KEYPAIR"

BASE_TOKENS=(
    # Original seed pairs
    "98sMhvDwXj1RQi5c5Mndm3vPe9cBqPrbLaufMXFNMh5g"  # HYPE
    "6p6xgHyF7AeE6TZkSmFsko444wqoP15icUSqi2jfGiPN"  # TRUMP
    "9BB6NFEcjBCtnNLFko2FqVQBq8HHM13kCyYcdQbgpump"  # FARTCOIN
    "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm"  # WIF
    "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"  # BONK
    "HZ1JovNiVvGrGNiiYvEozEVgZ58xaU3RKwX8eACQBCt3"  # PYTH
    # LST tokens
    "jtojtomepa8beP8AuQc6eXt5FriJwfFMwQx2v2f9mCL"  # JitoSOL
    "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So"  # mSOL
    "bSo13r4TkiE4KumL71LsHTPpL2euBYLFx6h9HP3piy1"  # bSOL
    # DeFi tokens
    "27G8MtK7VtTcCHkpASjSDdkWWYfoqT6ggEuKidVJidD4"  # JLP
    "TNSRxcUxoT9xBG3de7PiJyTDYu7kskLqcpddxnEJAS6"  # TNSR
    # Meme tokens
    "7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr"  # POPCAT
)

QUOTE_MINTS=(
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"  # USDC
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"  # USDT
    "So11111111111111111111111111111111111111112"     # WSOL
)

echo ""
echo "Tokens to pre-fund: ${#BASE_TOKENS[@]}"
echo "Quote mints: ${#QUOTE_MINTS[@]}"
# One ATA per mint per wallet; quote mint ATAs usually exist already.
echo "Total ATAs: ${#BASE_TOKENS[@]}"
echo ""
echo "Run this manually with:"
echo "  solana config set --url \$RPC_URL"
echo "  solana config set --keypair \$WALLET_KEYPAIR"
echo ""
echo "Then for each token:"
for mint in "${BASE_TOKENS[@]}"; do
    short="${mint:0:8}..."
    echo "  spl-token create-account $mint --owner \$(solana address)  # $short"
done

echo ""
echo "=== OR use this Node.js script ==="
echo "(requires @solana/web3.js and @solana/spl-token)"
echo ""
# ATA rent-exempt minimum in lamports; bash only does integer maths, so keep
# totals in lamports and let awk format the SOL value.
ATA_RENT_LAMPORTS=2039280
TOTAL_LAMPORTS=$(( ${#BASE_TOKENS[@]} * ATA_RENT_LAMPORTS ))
echo "Est. cost: ~$(awk -v l="$TOTAL_LAMPORTS" 'BEGIN { printf "%.6f", l / 1000000000 }') SOL"
echo "(~$TOTAL_LAMPORTS lamports total, $ATA_RENT_LAMPORTS lamports per ATA)"
