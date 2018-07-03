使用说明：
编写config文件，设定爬虫配置
创建类继承ccxt_spider.py class CCXTSpider
调用worker方法，传入交易所名称 exchange_name:"bithumb", "poloniex", "bigone", "okcoinusd","binance", "okex", "huobi",  "bitfinex","zb","kraken", "hitbtc"
要抓取的货币对list  symbols：["ETH/BTC","XRP/BTC",...]
抓取的类型   api_type: 目前只支持 ["trade","depth","ticker","OHLCV"]        
得到ccxt返回的data (set_trade_since方法传入时间戳，获取时间戳至今的时间数据，只支持trade和OHLCV)，自己实现data处理
返回的数据为{exchange_name + "_" + symbol + "_" + api : data}, 如{
                                                                "binance_ETH/BTC_trade":data,
                                                                "huobi_XRP/BTC_ticker":data,
                                                                ...
                                                                }