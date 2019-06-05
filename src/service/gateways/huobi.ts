/// <reference path="../utils.ts" />
/// <reference path="../../common/models.ts" />
/// <reference path="nullgw.ts" />
///<reference path="../config.ts"/>
///<reference path="../utils.ts"/>
///<reference path="../interfaces.ts"/>

import Q = require("q");
import crypto = require("crypto");
import request = require("request");
import url = require("url");
import querystring = require("querystring");
import Config = require("../config");
import NullGateway = require("./nullgw");
import Models = require("../../common/models");
import Utils = require("../utils");
import util = require("util");
import Interfaces = require("../interfaces");
import moment = require("moment");
import _ = require("lodash");
import log from "../logging";
var shortId = require("shortid");
var Deque = require("collections/deque");

interface HuobiMarketTrade {
    tid: number;
    timestamp: number;
    price: string;
    amount: string;
    exchange: string;
    type: string;
}

interface HuobiMarketLevel {
    price: string;
    amount: string;
    timestamp: string;
}

interface HuobiOrderBook {
    bids: HuobiMarketLevel[];
    asks: HuobiMarketLevel[];
}

function decodeSide(side: string) {
    switch (side) {
        case "buy": return Models.Side.Bid;
        case "sell": return Models.Side.Ask;
        default: return Models.Side.Unknown;
    }
}

function encodeSide(side: Models.Side) {
    switch (side) {
        case Models.Side.Bid: return "buy";
        case Models.Side.Ask: return "sell";
        default: return "";
    }
}

function encodeTimeInForce(tif: Models.TimeInForce, type: Models.OrderType) {
    if (type === Models.OrderType.Market) {
        return "exchange market";
    }
    else if (type === Models.OrderType.Limit) {
        if (tif === Models.TimeInForce.FOK) return "exchange fill-or-kill";
        if (tif === Models.TimeInForce.GTC) return "exchange limit";
    }
    throw new Error("unsupported tif " + Models.TimeInForce[tif] + " and order type " + Models.OrderType[type]);
}

class HuobiMarketDataGateway implements Interfaces.IMarketDataGateway {
    ConnectChanged = new Utils.Evt<Models.ConnectivityStatus>();

    private _since: number = null;
    MarketTrade = new Utils.Evt<Models.GatewayMarketTrade>();
    private onTrades = (trades: Models.Timestamped<HuobiMarketTrade[]>) => {
        _.forEach(trades.data, trade => {
            var px = parseFloat(trade.price);
            var sz = parseFloat(trade.amount);
            var time = moment.unix(trade.timestamp).toDate();
            var side = decodeSide(trade.type);
            var mt = new Models.GatewayMarketTrade(px, sz, time, this._since === null, side);
            this.MarketTrade.trigger(mt);
        });

        this._since = moment().unix();
    };

    private downloadMarketTrades = () => {
        var qs = { timestamp: this._since === null ? moment.utc().subtract(60, "seconds").unix() : this._since };
        this._http
            .get<HuobiMarketTrade[]>("trades/" + this._symbolProvider.symbol, qs)
            .then(this.onTrades)
            .done();
    };

    private static ConvertToMarketSide(level: HuobiMarketLevel): Models.MarketSide {
        return new Models.MarketSide(parseFloat(level.price), parseFloat(level.amount));
    }

    private static ConvertToMarketSides(level: HuobiMarketLevel[]): Models.MarketSide[] {
        return _.map(level, HuobiMarketDataGateway.ConvertToMarketSide);
    }

    MarketData = new Utils.Evt<Models.Market>();
    private onMarketData = (book: Models.Timestamped<HuobiOrderBook>) => {
        var bids = HuobiMarketDataGateway.ConvertToMarketSides(book.data.bids);
        var asks = HuobiMarketDataGateway.ConvertToMarketSides(book.data.asks);
        this.MarketData.trigger(new Models.Market(bids, asks, book.time));
    };

    private downloadMarketData = () => {
        this._http
            .get<HuobiOrderBook>("book/" + this._symbolProvider.symbol, { limit_bids: 5, limit_asks: 5 })
            .then(this.onMarketData)
            .done();
    };

    constructor(
        timeProvider: Utils.ITimeProvider,
        private _http: HuobiHttp,
        private _symbolProvider: HuobiSymbolProvider) {

        timeProvider.setInterval(this.downloadMarketData, moment.duration(5, "seconds"));
        timeProvider.setInterval(this.downloadMarketTrades, moment.duration(15, "seconds"));

        this.downloadMarketData();
        this.downloadMarketTrades();

        _http.ConnectChanged.on(s => this.ConnectChanged.trigger(s));
    }
}

interface RejectableResponse {
    message: string;
}

interface HuobiNewOrderRequest {
    symbol: string;
    amount: string;
    price: string; //Price to buy or sell at. Must be positive. Use random number for market orders.
    exchange: string; //always "Huobi"
    side: string; // buy or sell
    type: string; // "market" / "limit" / "stop" / "trailing-stop" / "fill-or-kill" / "exchange market" / "exchange limit" / "exchange stop" / "exchange trailing-stop" / "exchange fill-or-kill". (type starting by "exchange " are exchange orders, others are margin trading orders)
    is_hidden?: boolean;
}

interface HuobiNewOrderResponse extends RejectableResponse {
    order_id: string;
}

interface HuobiCancelOrderRequest {
    order_id: string;
}

interface HuobiCancelReplaceOrderRequest extends HuobiNewOrderRequest {
    order_id: string;
}

interface HuobiCancelReplaceOrderResponse extends HuobiCancelOrderRequest, RejectableResponse { }

interface HuobiOrderStatusRequest {
    order_id: string;
}

interface HuobiMyTradesRequest {
    symbol: string;
    timestamp: number;
}

interface HuobiMyTradesResponse extends RejectableResponse {
    price: string;
    amount: string;
    timestamp: number;
    exchange: string;
    type: string;
    fee_currency: string;
    fee_amount: string;
    tid: number;
    order_id: string;
}

interface HuobiOrderStatusResponse extends RejectableResponse {
    symbol: string;
    exchange: string; // bitstamp or Huobi
    price: number;
    avg_execution_price: string;
    side: string;
    type: string; // "market" / "limit" / "stop" / "trailing-stop".
    timestamp: number;
    is_live: boolean;
    is_cancelled: boolean;
    is_hidden: boolean;
    was_forced: boolean;
    executed_amount: string;
    remaining_amount: string;
    original_amount: string;
}

class HuobiOrderEntryGateway implements Interfaces.IOrderEntryGateway {
    OrderUpdate = new Utils.Evt<Models.OrderStatusUpdate>();
    ConnectChanged = new Utils.Evt<Models.ConnectivityStatus>();

    supportsCancelAllOpenOrders = (): boolean => { return false; };
    cancelAllOpenOrders = (): Q.Promise<number> => { return Q(0); };

    generateClientOrderId = () => shortId.generate();

    public cancelsByClientOrderId = false;

    private convertToOrderRequest = (order: Models.OrderStatusReport): HuobiNewOrderRequest => {
        return {
            amount: order.quantity.toString(),
            exchange: "Huobi",
            price: order.price.toString(),
            side: encodeSide(order.side),
            symbol: this._symbolProvider.symbol,
            type: encodeTimeInForce(order.timeInForce, order.type)
        };
    }

    sendOrder = (order: Models.OrderStatusReport) => {
        var req = this.convertToOrderRequest(order);

        this._http
            .post<HuobiNewOrderRequest, HuobiNewOrderResponse>("order/new", req)
            .then(resp => {
                if (typeof resp.data.message !== "undefined") {
                    this.OrderUpdate.trigger({
                        orderStatus: Models.OrderStatus.Rejected,
                        orderId: order.orderId,
                        rejectMessage: resp.data.message,
                        time: resp.time
                    });
                    return;
                }

                this.OrderUpdate.trigger({
                    orderId: order.orderId,
                    exchangeId: resp.data.order_id,
                    time: resp.time,
                    orderStatus: Models.OrderStatus.Working
                });
            }).done();

        this.OrderUpdate.trigger({
            orderId: order.orderId,
            computationalLatency: Utils.fastDiff(new Date(), order.time)
        });
    };

    cancelOrder = (cancel: Models.OrderStatusReport) => {
        var req = { order_id: cancel.exchangeId };
        this._http
            .post<HuobiCancelOrderRequest, any>("order/cancel", req)
            .then(resp => {
                if (typeof resp.data.message !== "undefined") {
                    this.OrderUpdate.trigger({
                        orderStatus: Models.OrderStatus.Rejected,
                        cancelRejected: true,
                        orderId: cancel.orderId,
                        rejectMessage: resp.data.message,
                        time: resp.time
                    });
                    return;
                }

                this.OrderUpdate.trigger({
                    orderId: cancel.orderId,
                    time: resp.time,
                    orderStatus: Models.OrderStatus.Cancelled
                });
            })
            .done();

        this.OrderUpdate.trigger({
            orderId: cancel.orderId,
            computationalLatency: Utils.fastDiff(new Date(), cancel.time)
        });
    };

    replaceOrder = (replace: Models.OrderStatusReport) => {
        this.cancelOrder(replace);
        this.sendOrder(replace);
    };

    private downloadOrderStatuses = () => {
        var tradesReq = { timestamp: this._since.unix(), symbol: this._symbolProvider.symbol };
        this._http
            .post<HuobiMyTradesRequest, HuobiMyTradesResponse[]>("mytrades", tradesReq)
            .then(resps => {
                _.forEach(resps.data, t => {

                    this._http
                        .post<HuobiOrderStatusRequest, HuobiOrderStatusResponse>("order/status", { order_id: t.order_id })
                        .then(r => {

                            this.OrderUpdate.trigger({
                                exchangeId: t.order_id,
                                lastPrice: parseFloat(t.price),
                                lastQuantity: parseFloat(t.amount),
                                orderStatus: HuobiOrderEntryGateway.GetOrderStatus(r.data),
                                averagePrice: parseFloat(r.data.avg_execution_price),
                                leavesQuantity: parseFloat(r.data.remaining_amount),
                                cumQuantity: parseFloat(r.data.executed_amount),
                                quantity: parseFloat(r.data.original_amount)
                            });

                        })
                        .done();
                });
            }).done();

        this._since = moment.utc();
    };

    private static GetOrderStatus(r: HuobiOrderStatusResponse) {
        if (r.is_cancelled) return Models.OrderStatus.Cancelled;
        if (r.is_live) return Models.OrderStatus.Working;
        if (r.executed_amount === r.original_amount) return Models.OrderStatus.Complete;
        return Models.OrderStatus.Other;
    }

    private _since = moment.utc();
    private _log = log("tribeca:gateway:HuobiOE");
    constructor(
        timeProvider: Utils.ITimeProvider,
        private _details: HuobiBaseGateway,
        private _http: HuobiHttp,
        private _symbolProvider: HuobiSymbolProvider) {

        _http.ConnectChanged.on(s => this.ConnectChanged.trigger(s));
        timeProvider.setInterval(this.downloadOrderStatuses, moment.duration(8, "seconds"));
    }
}


class RateLimitMonitor {
    private _log = log("tribeca:gateway:rlm");

    private _queue = Deque();
    private _durationMs: number;

    public add = () => {
        var now = moment.utc();

        while (now.diff(this._queue.peek()) > this._durationMs) {
            this._queue.shift();
        }

        this._queue.push(now);

        if (this._queue.length > this._number) {
            this._log.error("Exceeded rate limit", { nRequests: this._queue.length, max: this._number, durationMs: this._durationMs });
        }
    }

    constructor(private _number: number, duration: moment.Duration) {
        this._durationMs = duration.asMilliseconds();
    }
}

class HuobiHttp {
    ConnectChanged = new Utils.Evt<Models.ConnectivityStatus>();

    private _timeout = 15000;

    get = <T>(actionUrl: string, qs?: any): Q.Promise<Models.Timestamped<T>> => {
        const url = this._baseUrl + "/" + actionUrl;
        var opts = {
            timeout: this._timeout,
            url: url,
            qs: qs || undefined,
            method: "GET"
        };

        return this.doRequest<T>(opts, url);
    };

    // Huobi seems to have a race condition where nonces are processed out of order when rapidly placing orders
    // Retry here - look to mitigate in the future by batching orders?
    post = <TRequest, TResponse>(actionUrl: string, msg: TRequest): Q.Promise<Models.Timestamped<TResponse>> => {
        return this.postOnce<TRequest, TResponse>(actionUrl, _.clone(msg)).then(resp => {
            var rejectMsg: string = (<any>(resp.data)).message;
            if (typeof rejectMsg !== "undefined" && rejectMsg.indexOf("Nonce is too small") > -1)
                return this.post<TRequest, TResponse>(actionUrl, _.clone(msg));
            else
                return resp;
        });
    }

    private postOnce = <TRequest, TResponse>(actionUrl: string, msg: TRequest): Q.Promise<Models.Timestamped<TResponse>> => {
        msg["request"] = "/v1/" + actionUrl;
        msg["nonce"] = this._nonce.toString();
        this._nonce += 1;

        var payload = new Buffer(JSON.stringify(msg)).toString("base64");
        var signature = crypto.createHmac("sha384", this._secret).update(payload).digest('hex');

        const url = this._baseUrl + "/" + actionUrl;
        var opts: request.Options = {
            timeout: this._timeout,
            url: url,
            headers: {
                "X-BFX-APIKEY": this._apiKey,
                "X-BFX-PAYLOAD": payload,
                "X-BFX-SIGNATURE": signature
            },
            method: "POST"
        };

        return this.doRequest<TResponse>(opts, url);
    };

    private doRequest = <TResponse>(msg: request.Options, url: string): Q.Promise<Models.Timestamped<TResponse>> => {
        var d = Q.defer<Models.Timestamped<TResponse>>();

        this._monitor.add();
        request(msg, (err, resp, body) => {
            if (err) {
                this._log.error(err, "Error returned: url=", url, "err=", err);
                d.reject(err);
            }
            else {
                try {
                    var t = new Date();
                    var data = JSON.parse(body);
                    d.resolve(new Models.Timestamped(data, t));
                }
                catch (err) {
                    this._log.error(err, "Error parsing JSON url=", url, "err=", err, ", body=", body);
                    d.reject(err);
                }
            }
        });

        return d.promise;
    };

    private _log = log("tribeca:gateway:HuobiHTTP");
    private _baseUrl: string;
    private _apiKey: string;
    private _secret: string;
    private _nonce: number;

    constructor(config: Config.IConfigProvider, private _monitor: RateLimitMonitor) {
        this._baseUrl = config.GetString("HuobiHttpUrl")
        this._apiKey = config.GetString("HuobiKey");
        this._secret = config.GetString("HuobiSecret");

        this._nonce = new Date().valueOf();
        this._log.info("Starting nonce: ", this._nonce);
        setTimeout(() => this.ConnectChanged.trigger(Models.ConnectivityStatus.Connected), 10);
    }
}

interface HuobiPositionResponseItem {
    type: string;
    currency: string;
    amount: string;
    available: string;
}

class HuobiPositionGateway implements Interfaces.IPositionGateway {
    PositionUpdate = new Utils.Evt<Models.CurrencyPosition>();

    private onRefreshPositions = () => {
        this._http.post<{}, HuobiPositionResponseItem[]>("balances", {}).then(res => {
            _.forEach(_.filter(res.data, x => x.type === "exchange"), p => {
                var amt = parseFloat(p.amount);
                var cur = Models.toCurrency(p.currency);
                var held = amt - parseFloat(p.available);
                var rpt = new Models.CurrencyPosition(amt, held, cur);
                this.PositionUpdate.trigger(rpt);
            });
        }).done();
    }

    private _log = log("tribeca:gateway:HuobiPG");
    constructor(timeProvider: Utils.ITimeProvider, private _http: HuobiHttp) {
        timeProvider.setInterval(this.onRefreshPositions, moment.duration(15, "seconds"));
        this.onRefreshPositions();
    }
}

class HuobiBaseGateway implements Interfaces.IExchangeDetailsGateway {
    public get hasSelfTradePrevention() {
        return false;
    }

    name(): string {
        return "Huobi";
    }

    makeFee(): number {
        return 0.001;
    }

    takeFee(): number {
        return 0.002;
    }

    exchange(): Models.Exchange {
        return Models.Exchange.Huobi;
    }

    constructor(public minTickIncrement: number) { }
}

class HuobiSymbolProvider {
    public symbol: string;

    constructor(pair: Models.CurrencyPair) {
        this.symbol = Models.fromCurrency(pair.base).toLowerCase() + Models.fromCurrency(pair.quote).toLowerCase();
    }
}

class Huobi extends Interfaces.CombinedGateway {
    constructor(timeProvider: Utils.ITimeProvider, config: Config.IConfigProvider, symbol: HuobiSymbolProvider, pricePrecision: number) {
        const monitor = new RateLimitMonitor(60, moment.duration(1, "minutes"));
        const http = new HuobiHttp(config, monitor);
        const details = new HuobiBaseGateway(pricePrecision);

        const orderGateway = config.GetString("HuobiOrderDestination") == "Huobi"
            ? <Interfaces.IOrderEntryGateway>new HuobiOrderEntryGateway(timeProvider, details, http, symbol)
            : new NullGateway.NullOrderGateway();

        super(
            new HuobiMarketDataGateway(timeProvider, http, symbol),
            orderGateway,
            new HuobiPositionGateway(timeProvider, http),
            details);
    }
}

interface SymbolDetails {
    "base-currency": string,
    "quote-currency": string,
    "price-precision": number,
    "amount-precision" : number,
    "symbol-partition": string
}

export async function createHuobi(timeProvider: Utils.ITimeProvider, config: Config.IConfigProvider, pair: Models.CurrencyPair): Promise<Interfaces.CombinedGateway> {
    const detailsUrl = config.GetString("HuobiHttpUrl") + "/common/symbols";
    const symbolDetails = await Utils.getJSON<SymbolDetails[]>(detailsUrl);
    const symbol = new HuobiSymbolProvider(pair);

    for (let s of symbolDetails) {
        if (s["base-currency"] + s["quote-currency"] === symbol.symbol)
            return new Huobi(timeProvider, config, symbol, 10 ** (-1 * s["price-precision"]));
    }

    throw new Error("cannot match pair to a Huobi Symbol " + pair.toString());
}