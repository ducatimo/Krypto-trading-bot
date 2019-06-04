/// <reference path="../utils.ts" />
/// <reference path="../../common/models.ts" />
/// <reference path="nullgw.ts" />
///<reference path="../interfaces.ts"/>

process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';

import ws = require("ws");
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
import crc32 = require("crc-32");
import { Socket } from "net";
import pako = require("pako");

let shortId = require("shortid");


interface HuobiMessageIncomingMessage {
    channel?: string;
    table?: string;
    data?: any;
    event?: string;
    action?: string;
    success?: boolean;
}

//https://huobi.readme.io/reference#get_market-depth
interface HuobiDepthMessage {
    asks: [string, string][];
    bids: [string, string][];
    ts: string;
    checksum: number;
    instrument_id: string;
}

interface HuobiTradeMessage {
    trade_id: string;
    instrument_id: string;
    price: string;
    side: string;
    size: string;
    timestamp: string;
}

interface OrderAck {
    result?: boolean; // true or false
    order_id?: string;
    client_oid?: string;
    code?: number;
    message: string;
}

interface SignedMessage {
    api_key?: string;
    sign?: string;
}

interface Order extends SignedMessage {
    client_oid: string,
    type: string,
    side: string,
    instrument_id: string;
    margin_trading?: Int8Array,
    price?: string;
    size?: string;
    notional?: string;
}

interface Cancel extends SignedMessage {
    order_id?: string;
    instrument_id: string;
    client_oid?: string;
}


/*
{
  "status": "ok",
  "data": {
    "id": 59378,
    "symbol": "ethusdt",
    "account-id": 100009,
    "amount": "10.1000000000",
    "price": "100.1000000000",
    "created-at": 1494901162595,
    "type": "buy-limit",
    "filled-amount": "10.1000000000",
    "filled-cash-amount": "1011.0100000000",
    "filled-fees": "0.0202000000",
    "finished-at": 1494901400468,
    "source": "api",
    "state": "filled",
    "canceled-at": 0
  }
}
*/


interface HuobiOrderStatus {
    id: string,
    "account-id": string,
    price: string,
    size: string,
    notional: string,
    instrument_id: string,
    side: string,
    type: string,
    timestamp: string,
    filled_size: string,
    filled_notional: string,
    status: string,
    margin_trading: string
}

interface SubscriptionRequest extends SignedMessage { }

class HuobiWebsocket {

    send = <T>(operation: string, args: any, cb?: () => void) => {
        let subsReq: any = { op: operation };
        if (args !== null) subsReq.args = args;
        this._ws.send(JSON.stringify(subsReq), (e: Error) => {
            if (!e && cb) cb();
        });
    }

    login = (signer: HuobiMessageSigner, cb?: () => void) => {
        let timestamp = (Date.now() / 1000).toString();
        let loginChannel = [signer.apiKey,
            timestamp,
            signer.ComputeHmac256(timestamp + "GET" + "/users/self/verify")
        ];
        this.send("login", loginChannel);
        this._loginHandlers[shortId.generate()] = cb;
    }

    setHandler = <T>(channel: string, handler: (newMsg: Models.Timestamped<T>) => void) => {
        this._handlers[channel] = handler;
    }

    private onMessage = (raw: any) => {
        try {
            if (!(typeof raw === 'string')) raw = pako.inflateRaw(raw, { to: 'string' });
            // this._log.info("Okex websocket on message!");
            this.resetTimer();
            let t = Utils.date();

            if (typeof raw !== "undefined" && raw === this._heartbeatPong) return;
            let msg: HuobiMessageIncomingMessage = JSON.parse(raw);
            if (typeof msg.event !== "undefined" && msg.event == "subscribe") return;
            if (typeof msg.event !== "undefined" && msg.event == "unsubscribe") {
                setTimeout(() => { this.send("subscribe", [msg.channel]); }, 1000);
                return;
            }
            if (typeof msg.event !== "undefined" && msg.event == "login") {
                if (!msg.success) this._log.warn("Unsuccessful login!", msg);
                else {
                    this.LoggedIn = true;
                    this._log.info("Successfully login!", msg);
                    _.forEach(this._loginHandlers, handler => {
                        handler();
                        this._log.info("calling handler!");
                    });
                }
                return;
            }

            if (typeof msg.table !== "undefined" && msg.data !== "undefined" && msg.data.length > 0) {
                let handler: (x: Models.Timestamped<any>) => void;

                if (msg.table == "spot/depth" && msg.action === "update") {
                    handler = this._handlers["spot/depthUpdate"];
                } else if (msg.table == "spot/depth" && msg.action === "partial") {
                    handler = this._handlers["spot/depth"];
                } else if (msg.table == "spot/trade") {
                    handler = this._handlers["spot/trade"];
                } else if (msg.table == "spot/order")
                    handler = this._handlers["spot/order"];

                if (typeof handler === "undefined") {
                    this._log.warn("Got message on unknown topic", msg);
                    return;
                }
                if (msg.table == "spot/depth")
                    handler(new Models.Timestamped<HuobiDepthMessage>(msg.data[0], t));
                if (msg.table == "spot/trade")
                    handler(new Models.Timestamped<HuobiTradeMessage[]>(msg.data, t));
                if (msg.table == "spot/order")
                    handler(new Models.Timestamped<HuobiOrderStatus[]>(msg.data, t));
                return;
            }
        }
        catch (e) {
            this._log.error(e, "Error parsing msg %o", raw);
            throw e;
        }
    };

    private onOpen = () => {
        this._log.info("Huobi websocket on open!");
        this.ConnectChanged.trigger(Models.ConnectivityStatus.Connected);
        this.initTimer();
    }
    private onClose = (code: number, message: string) => {
        this._log.info("Huobi websocket on close! code: " + code + +"message :" + message);
        this._ws = undefined;
        if (this._interval) {
            clearInterval(this._interval);
            this._interval = null;
        }
        this.ConnectChanged.trigger(Models.ConnectivityStatus.Disconnected);
    }

    private initTimer = () => {
        this._interval = setInterval(() => { if (this._ws) { this._ws.send(this._heartbeatPing); } }, 25000);
    }

    private resetTimer = () => {
        if (this._interval) {
            clearInterval(this._interval);
            this._interval = null;
            this.initTimer();
        }
    }

    private close = () => {
        if (this._ws) {
            console.log(`Closing websocket connection...`);
            this._ws.close();
            if (this._interval) {
                clearInterval(this._interval);
                this._interval = null;
            }
            this._ws = undefined;
        }
    }

    ConnectChanged = new Utils.Evt<Models.ConnectivityStatus>();
    LoggedIn: boolean = false;
    private _heartbeatPing = "ping";
    private _heartbeatPong = "pong";
    private _log = log("tribeca:gateway:HuobiWebsocket");
    private _handlers: { [channel: string]: (newMsg: Models.Timestamped<any>) => void } = {};
    private _loginHandlers: { [rid: string]: () => void } = {};

    private _ws: ws;
    private _interval?: NodeJS.Timer | null;

    constructor(config: Config.IConfigProvider) {
        let okWs = config.GetString("HuobiWsUrl");
        this._ws = new ws(okWs);
        this._log.info({ "HuobiWsUrl": okWs }, "Constructing HuobiWebsocket!");
        this._ws.on("open", () => { this.onOpen(); });
        this._ws.on("message", msg => { this.onMessage(msg); });
        this._ws.on("close", (code, message) => { this.onClose(code, message); });
    }
}

class HuobiMarketDataGateway implements Interfaces.IMarketDataGateway {
    ConnectChanged = new Utils.Evt<Models.ConnectivityStatus>();

    MarketTrade = new Utils.Evt<Models.GatewayMarketTrade>();
    private onTrade = (trades: Models.Timestamped<HuobiTradeMessage[]>) => {
        _.forEach(trades.data, trade => {
            let px = parseFloat(trade.price);
            let amt = parseFloat(trade.size);
            let side = trade.side === "sell" ? Models.Side.Ask : Models.Side.Bid; // is this the make side?
            let mt = new Models.GatewayMarketTrade(px, amt, trades.time, trades.data.length > 0, side);
            this.MarketTrade.trigger(mt);
        });
    };

    // TODO: Sort order?
    MarketData = new Utils.Evt<Models.Market>();
    private _market: Models.Market = null;
    private static GetLevel = (n: [string, string, string]): Models.MarketSide => new Models.MarketSide(parseFloat(n[0]), parseFloat(n[1]));
    private onDepth = (depth: Models.Timestamped<HuobiDepthMessage>) => {
        let depthData = depth.data;
        let bids = _(depthData.bids).map(HuobiMarketDataGateway.GetLevel).value();
        let asks = _(depthData.asks).map(HuobiMarketDataGateway.GetLevel).value();
        this._market = new Models.Market(bids, asks, depth.time);
        this.MarketData.trigger(this._market);
    };

    private checksum = (bids: Models.MarketSide[], asks: Models.MarketSide[], c: number) => {
        if (bids == null || asks == null) return false;
        const buff = [];
        for (let i = 0; i < 25; i++) {
            if (bids[i]) {
                const bid = bids[i];
                buff.push(bid.price);
                buff.push(bid.size);
            }
            if (asks[i]) {
                const ask = asks[i];
                buff.push(ask.price);
                buff.push(ask.size);
            }
        }
        let checkString = buff.join(":");
        const checksum = crc32.str(checkString);
        return (checksum === c);
    }

    private merge = (update: Models.MarketSide[], origin: Models.MarketSide[], sort: number): Models.MarketSide[] => {
        let ret: Models.MarketSide[] = [];
        let ul = update.length;
        let ol = origin.length;
        let loop = ul + ol;
        for (let u = 0, o = 0; u < loop && o < loop; u++ , o++) {
            if (u < ul && o < ol) {
                if (update[u].price * sort > origin[o].price * sort) {
                    if (update[u].size > 0) { ret.push(update[u]); o--; }
                } else if (update[u].price * sort < origin[o].price * sort) {
                    if (origin[o].size > 0) { ret.push(origin[o]); u--; }
                } else if (update[u].size > 0) ret.push(update[u]);
            } else if (u >= ul && o < ol) {
                if (origin[o].size > 0) ret.push(origin[o]);
            } else if (u < ul && o >= ol) {
                if (update[u].size > 0) ret.push(update[u]);
            } else break;
        }
        return ret;
    }

    private onDepthUpdate = (depth: Models.Timestamped<HuobiDepthMessage>) => {
        if (depth == null || depth == undefined) {
            let depthChannel = ["spot/depth:" + this._symbolProvider.symbol];
            this._socket.send("unsubscribe", depthChannel);
            return;
        }
        let depthData = depth.data;
        let bidsUpdate = _(depthData.bids).map(HuobiMarketDataGateway.GetLevel).value();
        let asksUpdate = _(depthData.asks).map(HuobiMarketDataGateway.GetLevel).value();

        let newBids: Models.MarketSide[] = this.merge(bidsUpdate, this._market.bids, 1);
        let newAsks: Models.MarketSide[] = this.merge(asksUpdate, this._market.asks, -1);

        if (this.checksum(newBids, newAsks, depthData.checksum)) {
            let mkt = new Models.Market(newBids, newAsks, depth.time);
            this.MarketData.trigger(mkt);
            this._market = mkt;
        } else {
            let depthChannel = ["spot/depth:" + this._symbolProvider.symbol];
            this._socket.send("unsubscribe", depthChannel);
        }
    };
    private _log = log("tribeca:gateway:Huobi");
    constructor(private _socket: HuobiWebsocket, private _symbolProvider: HuobiSymbolProvider) {

        let depthChannel = ["spot/depth:" + _symbolProvider.symbol];
        let tradesChannel = ["spot/trade:" + _symbolProvider.symbol];

        _socket.setHandler("spot/depth", this.onDepth);
        _socket.setHandler("spot/depthUpdate", this.onDepthUpdate);
        _socket.setHandler("spot/trade", this.onTrade);
        // Note：_socket.ConnectChanged VS. this.ConnectChanged
        _socket.ConnectChanged.on(cs => {
            this.ConnectChanged.trigger(cs);
            if (cs == Models.ConnectivityStatus.Connected) {
                _socket.send("subscribe", depthChannel);
                _socket.send("subscribe", tradesChannel);
            } else {
                //TODO:
            }
        });
    }
}

class HuobiOrderEntryGateway implements Interfaces.IOrderEntryGateway {
    OrderUpdate = new Utils.Evt<Models.OrderStatusUpdate>();
    ConnectChanged = new Utils.Evt<Models.ConnectivityStatus>();

    generateClientOrderId = () => {
        return shortId.generate().replace(/_/g, "XX").replace(/-/g, "YY");
    };

    supportsCancelAllOpenOrders = (): boolean => { return false; };
    cancelAllOpenOrders = (): Q.Promise<number> => { return Q(0); };

    public cancelsByClientOrderId = true;
    // let's really hope there's no race conditions on their end -- we're assuming here that orders sent first
    // will be acked first, so we can match up orders and their acks
    private _ordersWaitingForAckQueue = [];

    sendOrder = (order: Models.OrderStatusReport) => {
        this._log.info("To Send Order [ %s ]", order.orderId);

        let o: Order = {
            instrument_id: this._symbolProvider.symbol,
            type: order.type === Models.OrderType.Limit ? "limit" : "market",
            side: order.side === Models.Side.Bid ? "buy" : "sell",
            client_oid: order.orderId,
        };

        if (order.type === Models.OrderType.Limit) {
            o.size = order.quantity.toString();
            o.price = order.price.toString();
        } else {
            o.size = order.quantity.toString();
            o.notional = (order.price * order.quantity).toString();
        }

        let jsonString = JSON.stringify(o);
        this._http.post("/api/spot/v3/orders", jsonString).then((msg: Models.Timestamped<OrderAck>) => {
            let orderAcceptTime = Utils.date();
            let osr: Models.OrderStatusUpdate = {
                exchange: order.exchange,
                pair: order.pair,
                side: order.side,
                source: order.source,
                type: order.type,
                timeInForce: order.timeInForce,
                preferPostOnly: order.preferPostOnly,
                quantity: order.quantity,
                price: order.price,
                time: order.time,
                computationalLatency: Utils.fastDiff(orderAcceptTime, order.time)
            };
            if (msg.data.result) {
                osr.orderId = msg.data.client_oid;
                osr.exchangeId = msg.data.order_id;
                osr.orderStatus = Models.OrderStatus.Working;
                osr.cancelRejected = false;
                this._log.info("Order Submited! [ %s || %s ]", msg.data.client_oid, msg.data.order_id);
            } else {
                osr.orderId = order.orderId;
                osr.exchangeId = order.exchangeId;
                osr.orderStatus = Models.OrderStatus.Cancelled;
                osr.cancelRejected = false;
                osr.rejectMessage = msg.data.message;
                osr.rejectCode = msg.data.code;
                this._log.warn("Order Rejected! [ %o ]", msg.data);
            }
            this.OrderUpdate.trigger(osr);
        }, (err) => {
            order.orderStatus = Models.OrderStatus.Other;
            this._log.warn("Submit order error or timeout! [ %o ]", err);
            this.OrderUpdate.trigger(order);

        }).done();
    };

    cancelOrder = (cancel: Models.OrderStatusReport) => {
        this._log.info("To Cancel Order [ %s || %s ] Status: %s --- PendingCancel: %s --- CancelRejected: %s --- Price: %s", cancel.orderId, cancel.exchangeId, cancel.orderStatus, cancel.pendingCancel, cancel.cancelRejected, cancel.price);

        let c: Cancel = { instrument_id: this._symbolProvider.symbol };

        let cancelId = this.cancelsByClientOrderId ? cancel.orderId : cancel.exchangeId;
        let jsonString = JSON.stringify(c);
        this._http.post("/api/spot/v3/cancel_orders/" + cancelId, jsonString)
            .then((msg: Models.Timestamped<OrderAck>) => {
                let osr: Models.OrderStatusUpdate = {
                    exchange: cancel.exchange,
                    pair: cancel.pair,
                    side: cancel.side,
                    source: cancel.source,
                    type: cancel.type,
                    timeInForce: cancel.timeInForce,
                    preferPostOnly: cancel.preferPostOnly,
                    quantity: cancel.quantity,
                    price: cancel.price,
                    time: cancel.time,
                };
                if (msg.data.result) {
                    osr.orderId = msg.data.client_oid;
                    osr.exchangeId = msg.data.order_id;
                    osr.orderStatus = Models.OrderStatus.Cancelled;
                    osr.cancelRejected = false;
                    osr.pendingCancel = false;
                    this._log.info("Order Cancelled! [ %s || %s ]", msg.data.client_oid, msg.data.order_id);
                } else {
                    osr.orderId = cancel.orderId;
                    osr.exchangeId = cancel.exchangeId;
                    osr.orderStatus = Models.OrderStatus.Rejected;
                    osr.rejectCode = msg.data.code;
                    osr.rejectMessage = msg.data.message;
                    osr.cancelRejected = true;
                    let errArray = [33027, 33026, 33014];
                    if (errArray.indexOf(osr.rejectCode) > -1) {
                        osr.orderStatus = Models.OrderStatus.Cancelled;
                        osr.pendingCancel = false;
                    }
                    this._log.warn("Cancel Order Rejected! [ %s||%s ]: %o", cancel.orderId, cancel.exchangeId, msg.data);
                }
                this.OrderUpdate.trigger(osr);
            }, (err) => {
                cancel.orderStatus = Models.OrderStatus.Other;
                cancel.cancelRejected = false;
                this._log.warn("Cancel order error or timeout! [ %o ]", err);
                this.OrderUpdate.trigger(cancel);
            })
            .done();
    };

    replaceOrder = (replace: Models.OrderStatusReport) => {
        this.cancelOrder(replace);
        this.sendOrder(replace);
    };

    // TODO:
    private static getStatus(status: string): Models.OrderStatus {
        // status: -1: cancelled, 0: pending, 1: partially filled, 2: fully filled, 4: cancel request in process
        switch (status) {
            case "cancelled": return Models.OrderStatus.Cancelled;
            case "open": return Models.OrderStatus.Working;
            case "part_filled": return Models.OrderStatus.Working;
            case "filled": return Models.OrderStatus.Complete;
            case "failure": return Models.OrderStatus.Rejected;
            default: return Models.OrderStatus.Other;
        }
    }

    // TODO: Trade information can be got from BOTH websocket and RESTful API.
    private onOrder = (msg: Models.Timestamped<HuobiOrderStatus[]>) => {
        let t = msg.time;
        let orders: HuobiOrderStatus[] = msg.data;
        _.forEach(orders, order => {
            let filledSize = parseFloat(order.filled_size);
            let size = parseFloat(order.size);
            let filledNotional = parseFloat(order.filled_notional);
            let avgPx = filledSize > 0 ? 0 : filledNotional / filledSize;
            let price = parseFloat(order.price);
            let lastPx = price;

            let status: Models.OrderStatusUpdate = {
                orderId: order.client_oid,
                exchangeId: order.order_id,
                orderStatus: HuobiOrderEntryGateway.getStatus(order.status),
                time: t,
                pair: this._symbolProvider.pair,
                side: order.side === "buy" ? Models.Side.Bid : Models.Side.Ask,
                type: order.type === "limit" ? Models.OrderType.Limit : Models.OrderType.Market,
                quantity: size,
                lastQuantity: filledSize,
                leavesQuantity: size - filledSize,
                cumQuantity: filledSize,
                lastPrice: lastPx > 0 ? lastPx : undefined,
                averagePrice: avgPx > 0 ? avgPx : undefined,
                partiallyFilled: order.status === "part_filled",
                liquidity: Models.Liquidity.Make,
                cancelRejected: false,
                pendingCancel: false,
                pendingReplace: false
            };
            this._log.info("Exchange Order [ %s || %s ] Status: %s --- PendingCancel: %s --- CancelRejected: %s --- Price: %s", status.orderId, status.exchangeId, status.orderStatus, status.pendingCancel, status.cancelRejected, status.price);

            this.OrderUpdate.trigger(status);
        })

    };

    private _log = log("tribeca:gateway:Huobi");
    constructor(
        private _http: HuobiHttp,
        private _socket: HuobiWebsocket,
        private _signer: HuobiMessageSigner,
        private _symbolProvider: HuobiSymbolProvider) {
        let orderChannel = ["spot/order:" + _symbolProvider.symbol];
        this._socket.setHandler("spot/order", this.onOrder);
        // Note：_socket.ConnectChanged VS. this.ConnectChanged
        this._socket.ConnectChanged.on(cs => {
            if (cs == Models.ConnectivityStatus.Connected) {
                if (this._socket.LoggedIn) {
                    this._socket.send("subscribe", orderChannel);
                } else {
                    this._socket.login(this._signer, () => {
                        this._socket.send("subscribe", orderChannel);
                        this._log.info(orderChannel, "subscribing orderChannel");
                    });
                }
                this.ConnectChanged.trigger(cs);// MARK: Login first
            } else {
                //TODO:
            }
        });
    }
}

//https://github.com/huobiapi/API_Docs_en/wiki/Signing_API_Requests
class HuobiMessageSigner {
    private _secretKey: string;
    private _api_key: string;

    public get apiKey(): string { return this._api_key; };

    public ComputeHmac256 = (message: string): string => {
        return crypto.createHmac("SHA256", this._secretKey).update(message).digest("base64");
    }

    public signMessage = (m: SignedMessage): SignedMessage => {
        let els: string[] = [];
        if (!m.hasOwnProperty("api_key"))
            m.api_key = this._api_key;
        let keys = [];
        for (let key in m) {
            if (m.hasOwnProperty(key))
                keys.push(key);
        }
        keys.sort();
        for (let i = 0; i < keys.length; i++) {
            const k = keys[i];
            if (m.hasOwnProperty(k))
                els.push(m[k]);
        }
        let sig = els.join("") + this._secretKey;
        m.sign = crypto.createHash("md5").update(sig).digest("hex").toString().toUpperCase();
        return m;
    };

    constructor(config: Config.IConfigProvider) {
        this._api_key = config.GetString("HuobiApiKey");
        this._secretKey = config.GetString("HuobiSecretKey");
    }
}


class HuobiHttp {
    post = <T>(actionUrl: string, jsonString: string): Q.Promise<Models.Timestamped<T>> => {
        let d = Q.defer<Models.Timestamped<T>>();
        let u = url.resolve(this._baseUrl, actionUrl);
        let timestamp = Utils.date().toISOString();
        let preHash = timestamp + "POST" + actionUrl + jsonString;
        request({
            url: u,
            body: jsonString,
            headers: {
                "Content-Type": "application/json",
                "AccessKeyId": this._signer.apiKey,
                "Signature": this._signer.ComputeHmac256(preHash),
                "Timestamp": timestamp,
            },
            method: "POST",
            timeout: 10 * 1000
        }, (err, resp, body) => {
            if (err) d.reject(err);
            else {
                try {
                    let t = Utils.date();
                    let jsonObj = JSON.parse(body);
                    d.resolve(new Models.Timestamped(jsonObj, t));
                }
                catch (e) {
                    this._log.error(err, "url: %s, err: %o, body: %o", actionUrl, err, body);
                    d.reject(e);
                }
            }
        });
        return d.promise;
    }

    get = <T>(actionUrl: string): Q.Promise<Models.Timestamped<T>> => {
        let d = Q.defer<Models.Timestamped<T>>();
        let u = url.resolve(this._baseUrl, actionUrl);
        let timestamp = Utils.date().toISOString();
        let preHash = timestamp + "GET" + actionUrl;
        request({
            url: u,
            headers: {
                "Content-Type": "application/json",
                "AccessKeyId": this._signer.apiKey,
                "Signature": this._signer.ComputeHmac256(preHash),
                "Timestamp": timestamp,
            },
            method: "GET",
            timeout: 10 * 1000

        }, (err, resp, body) => {
            if (err) d.reject(err);
            else {
                try {
                    let t = Utils.date();
                    let jsonObj = JSON.parse(body);
                    d.resolve(new Models.Timestamped<any>(jsonObj, t));
                }
                catch (e) {
                    this._log.error(err, "url: %s, err: %o, body: %o", actionUrl, err, body);
                    d.reject(e);
                }
            }
        });
        return d.promise;
    }

    private _log = log("tribeca:gateway:HuobiHTTP");
    private _baseUrl: string;
    constructor(config: Config.IConfigProvider, private _signer: HuobiMessageSigner) {
        this._baseUrl = config.GetString("HuobiHttpUrl");
        this._log.info({ "HuobiHttpUrl": this._baseUrl }, "Constructing HuobiHttp!");
    }
}

class HuobiPositionGateway implements Interfaces.IPositionGateway {
    PositionUpdate = new Utils.Evt<Models.CurrencyPosition>();

    private static convertCurrency(name: string): Models.Currency {
        switch (name.toLowerCase()) {
            case "btc": return Models.Currency.BTC;
            case "eth": return Models.Currency.ETH;
            case "eos": return Models.Currency.EOS;
            case "usdt": return Models.Currency.USDT;
            case "xrp": return Models.Currency.XRP;
            case "ltc": return Models.Currency.LTC;
            case "bnb": return Models.Currency.BNB;
            case "trx": return Models.Currency.TRX;
            case "usd": return Models.Currency.USD;
            case "eur": return Models.Currency.EUR;
            case "gbp": return Models.Currency.GBP;
            case "cny": return Models.Currency.CNY;
            case "dash": return Models.Currency.DASH;
            case "okb": return Models.Currency.OKB;

            default: throw new Error("Unsupported currency " + name);
        }
    }

    private trigger = () => {
        this._http.get("/api/spot/v3/accounts").then(msg => {
            let accountArray = <Array<any>>msg.data;
            accountArray.forEach(account => {
                let available = parseFloat(account.available);
                let held = parseFloat(account.hold);
                let currency = HuobiPositionGateway.convertCurrency(account.currency);
                let pos = new Models.CurrencyPosition(available, held, currency);
                this.PositionUpdate.trigger(pos);
            });
        }).done();
    };

    private _log = log("tribeca:gateway:HuobiPG");
    constructor(private _http: HuobiHttp) {
        setInterval(this.trigger, 15000);
        setTimeout(this.trigger, 10);
    }
}

class HuobiBaseGateway implements Interfaces.IExchangeDetailsGateway {
    public get hasSelfTradePrevention() { return false; }
    name(): string { return "Huobi"; }
    makeFee(): number { return 0.001; }
    takeFee(): number { return 0.001; }
    exchange(): Models.Exchange { return Models.Exchange.Okex; }
    constructor(public minTickIncrement: number) { }
}

class HuobiSymbolProvider {
    public symbol: string;
    public symbolWithoutHyphen: string;
    public pair: Models.CurrencyPair;

    constructor(pair: Models.CurrencyPair) {
        this.pair = pair;
        const GetCurrencySymbol = (s: Models.Currency): string => Models.fromCurrency(s);
        this.symbol = GetCurrencySymbol(pair.base) + "-" + GetCurrencySymbol(pair.quote);
        this.symbolWithoutHyphen = GetCurrencySymbol(pair.base) + GetCurrencySymbol(pair.quote);
    }
}

class Huobi extends Interfaces.CombinedGateway {
    constructor(config: Config.IConfigProvider, pair: Models.CurrencyPair) {
        let symbol = new HuobiSymbolProvider(pair);
        let signer = new HuobiMessageSigner(config);
        let http = new HuobiHttp(config, signer);
        let socket = new HuobiWebsocket(config);
        let minTickIncrement = config.GetNumber("MinTickIncrement");


        let orderGateway = config.GetString("HuobiOrderDestination") == "Okex"
            ? <Interfaces.IOrderEntryGateway>new HuobiOrderEntryGateway(http, socket, signer, symbol)
            : new NullGateway.NullOrderGateway();

        super(
            new HuobiMarketDataGateway(socket, symbol),
            orderGateway,
            new HuobiPositionGateway(http),
            new HuobiBaseGateway(minTickIncrement)); 
    }
}

export async function createHuobi(config: Config.IConfigProvider, pair: Models.CurrencyPair): Promise<Interfaces.CombinedGateway> {
    return new Huobi(config, pair);
}