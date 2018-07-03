import ccxt
import ccxt.async as ccxt_async
import redis
import asyncio
import traceback
import time
import datetime
from my_log import Logger
import config
import multiprocessing as mp
from multiprocessing.dummy import Queue
import aiohttp
import ssl
import certifi


class CCXTSpider(object):
    def __init__(self, redis_addr, config_dict, log_file_name):

        """
        redis_addr: redis地址
        config_dict: 爬虫配置信息字典
        log_file_name: 日志文件名

        """
        self.redis = redis.StrictRedis(connection_pool=redis.ConnectionPool.from_url(redis_addr))
        self.config_dict = config_dict
        self.trade_since = None
        self.ohlcv_timeframe = None
        self.symbol_queue = None
        self.api_key = None
        self.secret = None
        self.logger = Logger(logger_name=log_file_name, file_name=log_file_name)

    def set_trade_since(self, since):
        # 设置成交爬取的时间 ,since为时间戳
        self.trade_since = since

    # def set_ohlcv_timeframe(self, timeframe):
        # 设置K线爬取时间
        # self.ohlcv_timeframe = timeframe

    def get_utcnow_timestamp(self):
        """获得13位UTC时间戳"""
        now = datetime.datetime.utcnow()
        utc_timestamp = (now - datetime.datetime(1970, 1, 1)).total_seconds()
        return int(round(utc_timestamp * 1000))

    def set_symbols_mode(self, symbols, mode):
        """
        货币对采取多进程或多线程模式，使用队列
        :param symbols: 
        :param mode: process多进程,thread多线程
        :return: 
        """
        if mode == "process":
            manager = mp.Manager()
            queue = manager.Queue()
        else:
            queue = Queue()

        for symbol in symbols:
            queue.put(symbol)
        self.symbol_queue = queue

    def get_queue_symbol(self):
        """
        获取队列中的货币对
        :return: 货币对
        """
        symbol = self.symbol_queue.get()
        return symbol

    def set_api_key(self, api_key, secret):
        self.api_key = api_key
        self.secret = secret

    def _get_exchange(self, exchange_name, ip=None):

        """
        获取ccxt实例
        exchange_name: 交易所code
        ip: 是否使用出口ip
        return: ccxt的交易所实例

        """
        exchange_params = {
            'rateLimit': self.config_dict[exchange_name + "_rate_limit"],
            'timeout': self.config_dict[exchange_name + "_timeout"],
            'enableRateLimit': True,
            'tokenBucket': {
                'delay': self.config_dict[exchange_name + "_delay"],
                'defaultCost': self.config_dict[exchange_name + "_cost"],
            }
        }
        # 设置出口ip
        if ip:
            asyncio_loop = asyncio.get_event_loop()
            context = ssl.create_default_context(cafile=certifi.where())
            connector = aiohttp.TCPConnector(ssl_context=context, loop=asyncio_loop, local_addr=(ip, 0))
            session = aiohttp.ClientSession(loop=asyncio_loop, connector=connector)
        else:
            session = None

        if exchange_name == "huobi":
            exchange = eval("ccxt_async.{}({})".format(exchange_name + "pro", exchange_params))
        elif exchange_name == "hitbtc" or exchange_name == "bitfinex":
            exchange = eval("ccxt_async.{}({})".format(exchange_name + "2", exchange_params))
        else:
            exchange = eval("ccxt_async.{}({})".format(exchange_name, exchange_params))

        if session:
            exchange.session = session
        return exchange

    async def _fetch_order_book(self, exchange, symbol):
        try:
            order_book = await exchange.fetch_order_book(symbol)
            # self.logger.info("fetch %s order book success" % symbol)
            return order_book
        except Exception as error:
            # self.logger.warning("fetch order book timeout,retry")
            for i in range(config.retry_times):
                try:
                    order_book = await exchange.fetch_order_book(symbol)
                    # self.logger.info("fetch %s order book success" % symbol)
                    return order_book
                except Exception as e:
                    # self.logger.warning("fetch order book timeout,retry")
                    continue
            self.logger.error("%s %s depth retry failed: %s" % (exchange.__name__(), symbol, error))
            return None

    async def _fetch_ticker(self, exchange, symbol):
        try:
            ticker = await exchange.fetch_ticker(symbol)
            # self.logger.info("fetch %s ticker success" % symbol)
            return ticker
        except Exception as error:
            # self.logger.warning("fetch ticker timeout,retry")
            for i in range(config.retry_times):
                try:
                    ticker = await exchange.fetch_ticker(symbol)
                    # self.logger.info("fetch %s ticker success" % symbol)
                    return ticker
                except Exception as e:
                    # self.logger.warning("fetch ticker timeout,retry")
                    continue
            self.logger.error("%s %s ticker retry failed: %s" % (exchange.__repr__, symbol, error))
            return None

    async def _fetch_trades(self, exchange, symbol, since):
        try:
            trades = await exchange.fetch_trades(symbol, since=since)
            # self.logger.info("fetch %s trade success" % symbol)
            return trades
        except Exception as error:
            # self.logger.warning("fetch trade timeout,retry")
            for i in range(config.retry_times):
                try:
                    trades = await exchange.fetch_trades(symbol, since=since)
                    # self.logger.info("fetch %s ticker success" % symbol)
                    return trades
                except Exception as e:
                    # self.logger.warning("fetch trade timeout,retry")
                    continue
            self.logger.error("%s %s trade retry failed: %s" % (exchange.__repr__, symbol, error))
            return None

    async def _fetch_ohlcv(self, exchange, symbol, since):
        try:
            ohlcv = await exchange.fetch_ohlcv(symbol, since=since)
            # self.logger.info("fetch %s ohlcv success" % symbol)
            return ohlcv
        except Exception as error:
            # self.logger.warning("fetch ohlcv timeout,retry")
            for i in range(config.retry_times):
                time.sleep(0.5)
                try:
                    ohlcv = await exchange.fetch_ohlcv(symbol, since=since)
                    # self.logger.info("fetch %s ohlcv success" % symbol)
                    return ohlcv
                except Exception as e:
                    # self.logger.warning("fetch ohlcv timeout,retry")
                    continue
            self.logger.error("%s %s ohlcv retry failed: %s" % (exchange.__repr__, symbol, error))
            return None

    async def _fetch_balance(self, exchange):
        if not self.api_key:
            raise ValueError("no api_key set!")
        exchange.apiKey = self.api_key
        exchange.secret = self.secret
        try:
            balance = await exchange.fetch_balace()
            return balance
        except Exception as e:
            time.sleep(0.5)
            # self.logger.warning("fetch balance timeout,retry")
            for i in range(config.retry_times):
                try:
                    balance = await exchange.fetch_balace()
                    return balance
                except:
                    # self.logger.warning("fetch balance timeout,retry")
                    continue
            self.logger.error("fetch balace retry error: %s" % e)
            return None

    def _load_markets(self, exchange):
        try:
            asyncio.get_event_loop().run_until_complete(exchange.loadMarkets())
            return True
        except Exception as error:
            # self.logger.warning("load markets failed, retry %s" % error)
            for i in range(config.retry_times):
                time.sleep(0.5)
                try:
                    asyncio.get_event_loop().run_until_complete(exchange.loadMarkets())
                    return True
                except:
                    # self.logger.warning("load markets failed, retry")
                    continue
            self.logger.error("load market failed : %s" % error)
            return False

    def _fetch_data(self, exchange, symbol, api_type):

        """
        获取原始数据
        exchange: ccxt交易所实例
        symbol: 货币对code
        api_type: 需要爬取的数据list: depth 深度，ticker 行情  eg: ["depth", "ticker"]
        return :交易所数据字典, key为api_type的值

        """
        if not api_type:
            return False
        tasks = []
        print("start fetch data")
        for api in api_type:
            if api == "depth":
                tasks.append(asyncio.ensure_future(self._fetch_order_book(exchange, symbol)))
            elif api == "ticker":
                tasks.append(asyncio.ensure_future(self._fetch_ticker(exchange, symbol)))
            elif api == "trade":
                tasks.append(asyncio.ensure_future(self._fetch_trades(exchange, symbol, since=self.trade_since)))
            elif api == "OHLCV":
                tasks.append(asyncio.ensure_future(self._fetch_ohlcv(exchange, symbol, since=self.trade_since)))
            elif api == "balance":
                tasks.append(asyncio.ensure_future(self._fetch_balance(exchange)))

        asyncio.get_event_loop().run_until_complete(asyncio.wait(tasks))
        try:
            result = [task.result() for task in tasks]
            return result
        except:
            print(traceback.print_exc())
            raise RuntimeError("error while fetching data!")

    def worker(self, exchange_name, symbols, api_type):

        """
        exchange_name: 交易所code
        symbols: 需要爬取的货币对list
        api_type: 爬取数据的种类
        
        返回list：[{binance_ETH/BTC_trade:""},...]

        """
        exchange = self._get_exchange(exchange_name)
        # asyncio.get_event_loop().run_until_complete(exchange.loadMarkets())
        load_market = self._load_markets(exchange)
        if not load_market:
            asyncio.get_event_loop().run_until_complete(exchange.close())
            return None
        data = {}

        for symbol in symbols:
            results = self._fetch_data(exchange, symbol, api_type)
            
            i = 0
            for api in api_type:
                data[exchange_name + "_" + symbol + "_" + api] = results[i]
                i = i + 1
        asyncio.get_event_loop().run_until_complete(exchange.close())
        time.sleep(config.config_dict[exchange_name + "_sleep_time"])
        return data

    def thread_worker(self, exchange_name, symbol, api_type, ip=None):
        exchange = self._get_exchange(exchange_name, ip)
        # asyncio.get_event_loop().run_until_complete(exchange.loadMarkets())
        load_market = self._load_markets(exchange)
        # print("finish load market")
        self.logger.warning("%s start load market" % ip)
        if not load_market:
            self.logger.error("%s load market failed" % ip)
            asyncio.get_event_loop().run_until_complete(exchange.close())
            return None
        data = {}

        results = self._fetch_data(exchange, symbol, api_type)
        self.logger.warning("%s get results success" % ip)

        i = 0
        for api in api_type:
            data[exchange_name + "_" + symbol + "_" + api] = results[i]
            i = i + 1
        asyncio.get_event_loop().run_until_complete(exchange.close())
        self.logger.warning("%s get data finish" % ip)
        return data
