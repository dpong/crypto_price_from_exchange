# crypto_price_from_exchange

目前只有bitmex的資料。

輸入symbol和資料起始日期、時間，就會一路抓到底。

bitmex的api有次數的限制，超過會被鎖一個小時，所以每次request最好間隔1秒。
