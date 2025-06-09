#!/bin/bash
# slightly modified from https://raw.githubusercontent.com/binance/binance-public-data/refs/heads/master/shell/download-klines.sh
set -e
mkdir -p data
cd data
#
symbols=("BTCUSDT") # add symbols here to download
# intervals=("1m" "3m" "5m" "15m" "30m" "1h" "2h" "4h" "6h" "8h" "12h" "1d" "3d" "1w" "1mo")
intervals=("15m") # change interval as you wish 1m, 5m, 15m etc.
years=("2024" "2025")
months=(01 02 03 04 05 06 07 08 09 10 11 12)

# baseurl="https://data.binance.vision/data/spot/monthly/klines"

baseurl="https://data.binance.vision/data/futures/um/monthly/klines"

for symbol in ${symbols[@]}; do
  for interval in ${intervals[@]}; do
    for year in ${years[@]}; do
      for month in ${months[@]}; do
        url="${baseurl}/${symbol}/${interval}/${symbol}-${interval}-${year}-${month}.zip"
        response=$(wget --server-response -c -q ${url} 2>&1 | awk 'NR==1{print $2}')
        if [ ${response} == '404' ]; then
          echo "File not exist: ${url}" 
        else
          echo "downloaded: ${url}"
        fi
      done
    done
  done
done  

for f in *.zip; do
  unzip "$f"
done

rm *.zip
cd ..
python convert_csv_to_parquet.py