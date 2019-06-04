/// <reference path="utils.ts" />
/// <reference path="../common/models.ts" />
/// <reference path="../common/messaging.ts" />
/// <reference path="utils.ts"/>
/// <reference path="interfaces.ts"/>
/// <reference path="persister.ts"/>
/// <reference path="messages.ts"/>

import Models = require("../common/models");
import Messaging = require("../common/messaging");
import Utils = require("./utils");
import _ = require("lodash");
import mongodb = require('mongodb');
import Q = require("q");
import Interfaces = require("./interfaces");
import Persister = require("./persister");
import util = require("util");
import Messages = require("./messages");
import * as moment from "moment";
import log from "./logging";

export class MarketDataBroker implements Interfaces.IMarketDataBroker {
    MarketData = new Utils.Evt<Models.Market>();
    public get currentBook(): Models.Market { return this._currentBook; }

    private _currentBook: Models.Market = null;
    private handleMarketData = (book: Models.Market) => {
        this._currentBook = book;
        this.MarketData.trigger(this.currentBook);
    };

    constructor(time: Utils.ITimeProvider,
        private _mdGateway: Interfaces.IMarketDataGateway,
        rawMarketPublisher: Messaging.IPublish<Models.Market>,
        persister: Persister.IPersist<Models.Market>,
        private _messages: Messages.MessagesPubisher) {

        time.setInterval(() => {
            if (!this.currentBook) return;
            rawMarketPublisher.publish(this._currentBook);
            persister.persist(new Models.Market(
                _.take(this.currentBook.bids, 3),
                _.take(this.currentBook.asks, 3),
                new Date()));
        }, moment.duration(1, "second"));

        rawMarketPublisher.registerSnapshot(() => this.currentBook === null ? [] : [this.currentBook]);

        this._mdGateway.MarketData.on(this.handleMarketData);
        this._mdGateway.ConnectChanged.on(s => {
            if (s == Models.ConnectivityStatus.Disconnected) this._currentBook = null;
            _messages.publish("MD gw " + Models.ConnectivityStatus[s]);
        });
    }
}

// export class OrderStateCache implements Interfaces.IOrderStateCache {
//     public allOrders = new Map<string, Models.OrderStatusReport>();
//     public exchIdsToClientIds = new Map<string, string>();
//     constructor(public persister: Persister.IPersist<Models.OrderStatusReport>) { }
// }

export class OrderBroker implements Interfaces.IOrderBroker {
    private _log = log("oe:broker");
    BrokerOrderUpdate = new Utils.Evt<Models.OrderStatusReport>();
    private _cancelsWaitingForExchangeOrderId: { [clId: string]: Models.OrderCancel } = {};
    private _cancelsWaitingForOrderAccepted: { [clId: string]: Models.OrderCancel } = {};

    Trade = new Utils.Evt<Models.Trade>();
    _trades: Models.Trade[] = [];
    private _pendingRemovals = new Array<Models.OrderStatusReport>();


    constructor(private _timeProvider: Utils.ITimeProvider,
        private _baseBroker: Interfaces.IBroker,
        private _oeGateway: Interfaces.IOrderEntryGateway,
        private _orderPersister: Persister.IPersist<Models.OrderStatusReport>,
        private _tradePersister: Persister.IPersist<Models.Trade>,
        private _orderStatusCachePersister: Persister.ILoadLatest<Models.OrderCachePersistable>,
        private _orderStatusPublisher: Messaging.IPublish<Models.OrderStatusReport>,
        private _tradePublisher: Messaging.IPublish<Models.Trade>,
        private _submittedOrderReciever: Messaging.IReceive<Models.OrderRequestFromUI>,
        private _cancelOrderReciever: Messaging.IReceive<Models.OrderStatusReport>,
        private _cancelAllOrdersReciever: Messaging.IReceive<Models.CancelAllOrdersRequest>,
        private _messages: Messages.MessagesPubisher,
        private _orderCache: Models.OrderStateCache,
        initOrders: Models.OrderStatusReport[],
        initTrades: Models.Trade[],
        private readonly _publishAllOrders: boolean) {

        _.each(initTrades, t => this._trades.push(t));

        this._oeGateway.ConnectChanged.on(cs => {

            if (cs == Models.ConnectivityStatus.Connected) {

                _orderStatusCachePersister.loadLatest().then((orderCache) => {
                    this._log.info("Original Cache Size = %s", orderCache.allOrders.length);

                    orderCache.allOrders.forEach((order) => {
                        this._log.info("Loading Cache! [ %s || %s ] Status: %s --- PendingCancel: %s --- CancelRejected: %s --- Price: %s", order.orderId, order.exchangeId, order.orderStatus, order.pendingCancel, order.cancelRejected, order.price);

                        if (!Models.orderIsDone(order.orderStatus)) {
                            this._orderCache.allOrders.set(order.orderId, order);
                            this._orderCache.exchIdsToClientIds.set(order.exchangeId, order.orderId);
                        } else if (order.orderStatus === Models.OrderStatus.Rejected && !order.cancelRejected) {
                            this._orderCache.allOrders.set(order.orderId, order);
                            this._orderCache.exchIdsToClientIds.set(order.exchangeId, order.orderId);
                        }
                    });

                    this._log.info("Loaded Cache Size = %s", _orderCache.allOrders.size);
                    this.cancelOpenOrders(true);
                }, (err) => {
                    this._log.warn("Failed to load Order Cache! [ %o ]", err);
                });
            }
        });


        _orderStatusPublisher.registerSnapshot(() => this.orderStatusSnapshot());
        _tradePublisher.registerSnapshot(() => _.takeRight(this._trades, 100));

        _submittedOrderReciever.registerReceiver((o: Models.OrderRequestFromUI) => {
            this._log.info("got new order req", o);
            try {
                const order = new Models.SubmitNewOrder(Models.Side[o.side], o.quantity, Models.OrderType[o.orderType],
                    o.price, Models.TimeInForce[o.timeInForce], this._baseBroker.exchange(), _timeProvider.utcNow(),
                    false, Models.OrderSource.OrderTicket);
                this.sendOrder(order);
            } catch (e) {
                this._log.error(e, "unhandled exception while submitting order", o);
            }
        });

        _cancelOrderReciever.registerReceiver(o => {
            this._log.info("got new cancel req", o);
            try {
                this.cancelOrder(new Models.OrderCancel(o.orderId, o.exchange, _timeProvider.utcNow()));
            } catch (e) {
                this._log.error(e, "unhandled exception while submitting order", o);
            }
        });

        _cancelAllOrdersReciever.registerReceiver(o => {
            this._log.info("handling cancel all orders request");
            this.cancelOpenOrders(false)
                .then(x => this._log.info("cancelled all ", x, " open orders"),
                    e => this._log.error(e, "error when cancelling all orders!"));
        });

        this._oeGateway.OrderUpdate.on(this.updateOrderState);
        this._oeGateway.ConnectChanged.on(s => {
            _messages.publish("OE gw " + Models.ConnectivityStatus[s]);
        });
        this._timeProvider.setInterval(this.clearPendingRemovals, moment.duration(5, "seconds"));
    }

    async cancelOpenOrders(cancelPendingCancel: boolean): Promise<number> {
        if (this._oeGateway.supportsCancelAllOpenOrders()) {
            return this._oeGateway.cancelAllOpenOrders();
        }
        const promiseMap = new Map<string, Q.Deferred<void>>();
        const orderUpdate = (o: Models.OrderStatusReport) => {
            const p = promiseMap.get(o.orderId);
            if (p && Models.orderIsDone(o.orderStatus))
                p.resolve(null);
        };
        this.BrokerOrderUpdate.on(orderUpdate);
        for (let e of this._orderCache.allOrders.values()) {
            if (e.orderStatus === Models.OrderStatus.Complete
                || e.orderStatus === Models.OrderStatus.Cancelled)
                continue;
            else if (!cancelPendingCancel && e.pendingCancel) continue;
            // Try to cancel ALL unfinished orders.(New,Working,Other)
            if (typeof e.exchangeId === "undefined" || e.exchangeId === null)
                e.orderStatus = Models.OrderStatus.New;
            else
                e.orderStatus = Models.OrderStatus.Working;

            this.cancelOrder(new Models.OrderCancel(e.orderId, e.exchange, this._timeProvider.utcNow()));
            promiseMap.set(e.orderId, Q.defer<void>());
        }
        const promises = Array.from(promiseMap.values());
        await Q.all(promises);
        this.BrokerOrderUpdate.off(orderUpdate);
        return promises.length;
    }

    private roundPrice = (price: number, side: Models.Side): number => {
        return Utils.roundSide(price, this._baseBroker.minTickIncrement, side);
    }

    sendOrder = (order: Models.SubmitNewOrder): Models.SentOrder => {
        const orderId = this._oeGateway.generateClientOrderId();

        const rpt: Models.OrderStatusUpdate = {
            pair: this._baseBroker.pair,
            orderId: orderId,
            side: order.side,
            quantity: order.quantity,
            type: order.type,
            price: this.roundPrice(order.price, order.side),
            timeInForce: order.timeInForce,
            orderStatus: Models.OrderStatus.New,
            preferPostOnly: order.preferPostOnly,
            exchange: this._baseBroker.exchange(),
            rejectMessage: order.msg,
            source: order.source,
            cancelRejected: false,
            pendingReplace: false,
            pendingCancel: false
        };
        this._oeGateway.sendOrder(this.updateOrderState(rpt));
        return new Models.SentOrder(rpt.orderId);
    };

    replaceOrder = (replace: Models.CancelReplaceOrder): Models.SentOrder => {
        const rpt = this._orderCache.allOrders.get(replace.origOrderId);
        if (!rpt) {
            throw new Error("Unknown order, cannot replace " + replace.origOrderId);
        }
        const report: Models.OrderStatusUpdate = {
            orderId: replace.origOrderId,
            orderStatus: Models.OrderStatus.Working,
            pendingReplace: true,
            pendingCancel: false,
            cancelRejected: false,
            price: this.roundPrice(replace.price, rpt.side),
            quantity: replace.quantity
        };
        this._oeGateway.replaceOrder(this.updateOrderState(rpt));
        return new Models.SentOrder(report.orderId);
    };

    //MARK: new,working,rejected
    cancelOrder = (cancel: Models.OrderCancel) => {
        const rpt = this._orderCache.allOrders.get(cancel.origOrderId);
        // if (!rpt) throw new Error("Unknown order, cannot cancel " + cancel.origOrderId);
        if (!rpt) {
            this._log.warn("Unknown Order %s", cancel.origOrderId);
            return;
        }
        //Order is done
        if (rpt.orderStatus === Models.OrderStatus.Complete
            || rpt.orderStatus === Models.OrderStatus.Cancelled) return;

        //New order
        if (rpt.orderStatus === Models.OrderStatus.New
            || typeof rpt.exchangeId === "undefined"
            || rpt.exchangeId === null) {
            if (!this._oeGateway.cancelsByClientOrderId) {
                this._cancelsWaitingForExchangeOrderId[rpt.orderId] = cancel;
                this._log.info("Registered %s for late deletion", rpt.orderId);
            } else {
                this._cancelsWaitingForOrderAccepted[rpt.orderId] = cancel;
                this._log.info("Registered %s for late deletion", rpt.orderId);
            }
            return;
        }


        //captured by updateOrderState -> working
        let report: Models.OrderStatusUpdate = {
            orderId: cancel.origOrderId,
            exchangeId: rpt.exchangeId,
            orderStatus: rpt.orderStatus,
            pendingCancel: true,
            cancelRejected: false,
            pendingReplace: false
        };
        let updatedOrderStated = this.updateOrderState(report);
        this._oeGateway.cancelOrder(updatedOrderStated);
    };

    public updateOrderState = (osr: Models.OrderStatusUpdate): Models.OrderStatusReport => {

        let orig: Models.OrderStatusUpdate = this.getOrderFromMemory(osr);
        if (typeof orig === "undefined") {
            this._log.error({
                update: osr,
                existingExchangeIdsToClientIds: this._orderCache.exchIdsToClientIds,
                existingIds: Array.from(this._orderCache.allOrders.keys())
            }, "no existing order for non-New update!");
            return;
        }

        let o: Models.OrderStatusReport = this.getOrFallback(osr, orig);

        // MARK: Add order to cache OR delete it from cache.
        const added = this.updateOrderInMemory(o);
        if (this._log.debug()) this._log.debug(o, (added ? "added" : "removed") + " order status");

        // MARK: Publish and persist order
        this.BrokerOrderUpdate.trigger(o);
        this._orderPersister.persist(o);

        if (this.shouldPublish(o))
            this._orderStatusPublisher.publish(o);

        if (osr.lastQuantity > 0) {
            let value = Math.abs(o.lastPrice * o.lastQuantity);

            const liq = o.liquidity;
            let feeCharged = null;
            if (typeof liq !== "undefined") {
                // negative fee is a rebate, positive fee is a fee
                feeCharged = (liq === Models.Liquidity.Make ? this._baseBroker.makeFee() : this._baseBroker.takeFee());
                const sign = (o.side === Models.Side.Bid ? 1 : -1);
                value = value * (1 + sign * feeCharged);
            }

            const trade = new Models.Trade(o.orderId + "." + o.version, o.time, o.exchange, o.pair,
                o.lastPrice, o.lastQuantity, o.side, value, o.liquidity, feeCharged);
            this.Trade.trigger(trade);
            this._tradePublisher.publish(trade);
            this._tradePersister.persist(trade);
            this._trades.push(trade);
        }
        return o;
    };

    private getOrderFromMemory = (osr: Models.OrderStatusUpdate): Models.OrderStatusUpdate => {
        this._log.info("Cache Size = %s", this._orderCache.allOrders.size);
        let orig: Models.OrderStatusUpdate;
        if (osr.orderStatus === Models.OrderStatus.New) {
            orig = osr;
        } else {
            orig = this._orderCache.allOrders.get(osr.orderId);
            if (typeof orig === "undefined") {
                const secondChance = this._orderCache.exchIdsToClientIds.get(osr.exchangeId);
                if (typeof secondChance !== "undefined") {
                    osr.orderId = secondChance;
                    orig = this._orderCache.allOrders.get(secondChance);
                }
            }
            if (typeof orig === "undefined") {
                const now = new Date().getTime();
                for (let order of this._orderCache.allOrders.values()) {
                    if ((typeof order.exchangeId === "undefined" || order.exchangeId === null)
                        && order.exchange === osr.exchange
                        && order.pair === osr.pair
                        && order.side === osr.side
                        && order.price === osr.price
                        && order.quantity === osr.quantity
                        && now - order.time.getTime() > 10 * 1000) {
                        order.exchangeId = osr.exchangeId;
                        osr.orderId = order.orderId;
                        orig = order;
                        break;
                    }
                }
            }
        }
        return orig;
    }

    private updateOrderInMemory = (osr: Models.OrderStatusReport): boolean => {
        //TODO: if (this.shouldPublish(osr) || !Models.orderIsDone(osr.orderStatus)) {
        if (!Models.orderIsDone(osr.orderStatus)) {
            if (!this._oeGateway.cancelsByClientOrderId
                && typeof osr.exchangeId !== "undefined"
                && osr.orderId in this._cancelsWaitingForExchangeOrderId) {
                this._log.info("Deleting %s late, oid: %s", osr.exchangeId, osr.orderId);
                const cancel = this._cancelsWaitingForExchangeOrderId[osr.orderId];
                delete this._cancelsWaitingForExchangeOrderId[osr.orderId];
                this.cancelOrder(cancel);
            } else if ((osr.orderStatus === Models.OrderStatus.Working || osr.orderStatus === Models.OrderStatus.New)
                && osr.orderId in this._cancelsWaitingForOrderAccepted) {
                this._log.info("Deleting %s late, oid: %s", osr.exchangeId, osr.orderId);
                const cancel = this._cancelsWaitingForOrderAccepted[osr.orderId];
                delete this._cancelsWaitingForOrderAccepted[osr.orderId];
                this.cancelOrder(cancel);
            } else if (osr.orderStatus === Models.OrderStatus.Other
                && osr.orderId in this._cancelsWaitingForOrderAccepted) {
                this._log.info("Still no exchangeId. Deleting %s again in 10s!", osr.orderId);
                const cancel = this._cancelsWaitingForOrderAccepted[osr.orderId];
                delete this._cancelsWaitingForOrderAccepted[osr.orderId];
                setTimeout(() => { this.cancelOrder(cancel); }, 10 * 1000);
            } else if (osr.orderStatus === Models.OrderStatus.Other) {
                osr.orderStatus = Models.OrderStatus.Working;
                this._log.info("Deleting %s again in 10s!", osr.orderId);
                const cancel = new Models.OrderCancel(osr.orderId, osr.exchange, this._timeProvider.utcNow());
                setTimeout(() => { this.cancelOrder(cancel); }, 10 * 1000);
            }
            this.addOrderInMemory(osr);
            return true;
        } else {
            if (osr.orderStatus === Models.OrderStatus.Rejected && osr.cancelRejected) {
                this.addOrderInMemory(osr);
                this._log.info("Retry to cancel %s", osr.orderId);
                osr.orderStatus = Models.OrderStatus.Working;
                osr.cancelRejected = false;
                let cancel = new Models.OrderCancel(osr.orderId, osr.exchange, this._timeProvider.utcNow());
                this.cancelOrder(cancel);
                return true;
            }
            this._pendingRemovals.push(osr);
            return false;
        }
    };

    private addOrderInMemory = (osr: Models.OrderStatusReport) => {
        this._log.info("Add %s oid: %s to orderCache", osr.exchangeId, osr.orderId);
        this._orderCache.exchIdsToClientIds.set(osr.exchangeId, osr.orderId);
        this._orderCache.allOrders.set(osr.orderId, osr);
    };

    private clearPendingRemovals = () => {
        const now = new Date().getTime();
        const kept = new Array<Models.OrderStatusReport>();
        for (let osr of this._pendingRemovals) {
            if (now - osr.time.getTime() > 5 * 1000) {
                this._orderCache.exchIdsToClientIds.delete(osr.exchangeId);
                this._orderCache.allOrders.delete(osr.orderId);
            } else
                kept.push(osr);
        }
        this._pendingRemovals = kept;
    };
    //TODO: modified to allow publishing the ongoing quotes.
    private shouldPublish = (o: Models.OrderStatusReport): boolean => {
        if (o.source === null) throw Error(JSON.stringify(o));
        if (this._publishAllOrders) return true;
        switch (o.source) {
            case Models.OrderSource.Quote:
            case Models.OrderSource.Unknown:
                return false;
            default:
                return true;
        }
    };


    private getOrFallback = (osr: Models.OrderStatusUpdate, orig: Models.OrderStatusUpdate): Models.OrderStatusReport => {
        let getOrFallback = <T>(n: T, o: T) => typeof n !== "undefined" ? n : o;
        let q = getOrFallback(osr.quantity, orig.quantity);
        let cumQ: number = (typeof osr.cumQuantity) !== "undefined" ? osr.cumQuantity : (getOrFallback(orig.cumQuantity, 0) + getOrFallback(osr.lastQuantity, 0));
        let o: Models.OrderStatusReport = {
            pair: getOrFallback(osr.pair, orig.pair),
            side: getOrFallback(osr.side, orig.side),
            quantity: q,
            type: getOrFallback(osr.type, orig.type),
            price: getOrFallback(osr.price, orig.price),
            timeInForce: getOrFallback(osr.timeInForce, orig.timeInForce),
            orderId: getOrFallback(osr.orderId, orig.orderId),
            exchangeId: getOrFallback(osr.exchangeId, orig.exchangeId),
            orderStatus: getOrFallback(osr.orderStatus, orig.orderStatus),
            rejectCode: osr.rejectCode,
            rejectMessage: osr.rejectMessage,
            time: getOrFallback(osr.time, this._timeProvider.utcNow()),
            lastQuantity: osr.lastQuantity,
            lastPrice: osr.lastPrice,
            leavesQuantity: getOrFallback(osr.leavesQuantity, orig.leavesQuantity),
            cumQuantity: cumQ,
            averagePrice: cumQ > 0 ? osr.averagePrice || orig.averagePrice : undefined,
            liquidity: getOrFallback(osr.liquidity, orig.liquidity),
            exchange: getOrFallback(osr.exchange, orig.exchange),
            computationalLatency: getOrFallback(osr.computationalLatency, 0) + getOrFallback(orig.computationalLatency, 0),
            version: (typeof orig.version === "undefined") ? 0 : orig.version + 1,
            partiallyFilled: cumQ > 0 && cumQ !== q,
            pendingCancel: getOrFallback(osr.pendingCancel, orig.pendingCancel),
            pendingReplace: getOrFallback(osr.pendingReplace, orig.pendingReplace),
            cancelRejected: getOrFallback(osr.cancelRejected, orig.cancelRejected),
            preferPostOnly: getOrFallback(osr.preferPostOnly, orig.preferPostOnly),
            source: getOrFallback(osr.source, orig.source)
        };
        return o;
    }

    private orderStatusSnapshot = (): Models.OrderStatusReport[] => {
        return Array.from(this._orderCache.allOrders.values()).filter(this.shouldPublish);
    }
}

export class PositionBroker implements Interfaces.IPositionBroker {
    private _log = log("pos:broker");

    public NewReport = new Utils.Evt<Models.PositionReport>();

    private _report: Models.PositionReport = null;
    public get latestReport(): Models.PositionReport {
        return this._report;
    }

    private _currencies: { [currency: number]: Models.CurrencyPosition } = {};
    public getPosition(currency: Models.Currency): Models.CurrencyPosition {
        return this._currencies[currency];
    }

    private onPositionUpdate = (rpt: Models.CurrencyPosition) => {
        this._currencies[rpt.currency] = rpt;
        const basePosition = this.getPosition(this._base.pair.base);
        const quotePosition = this.getPosition(this._base.pair.quote);

        if (typeof basePosition === "undefined"
            || typeof quotePosition === "undefined"
            || this._mdBroker.currentBook === null
            || this._mdBroker.currentBook.bids.length === 0
            || this._mdBroker.currentBook.asks.length === 0)
            return;

        const baseAmount = basePosition.amount;
        const quoteAmount = quotePosition.amount;
        const mid = (this._mdBroker.currentBook.bids[0].price + this._mdBroker.currentBook.asks[0].price) / 2.0;
        const baseValue = baseAmount + quoteAmount / mid + basePosition.heldAmount + quotePosition.heldAmount / mid;
        const quoteValue = baseAmount * mid + quoteAmount + basePosition.heldAmount * mid + quotePosition.heldAmount;
        const positionReport = new Models.PositionReport(baseAmount, quoteAmount, basePosition.heldAmount,
            quotePosition.heldAmount, baseValue, quoteValue, this._base.pair, this._base.exchange(), this._timeProvider.utcNow());

        if (this._report !== null &&
            Math.abs(positionReport.value - this._report.value) < 2e-2 &&
            Math.abs(baseAmount - this._report.baseAmount) < 2e-2 &&
            Math.abs(positionReport.baseHeldAmount - this._report.baseHeldAmount) < 2e-2 &&
            Math.abs(positionReport.quoteHeldAmount - this._report.quoteHeldAmount) < 2e-2)
            return;

        this._report = positionReport;
        this.NewReport.trigger(positionReport);
        this._positionPublisher.publish(positionReport);
        this._positionPersister.persist(positionReport);
    };

    constructor(private _timeProvider: Utils.ITimeProvider,
        private _base: Interfaces.IBroker,
        private _posGateway: Interfaces.IPositionGateway,
        private _positionPublisher: Messaging.IPublish<Models.PositionReport>,
        private _positionPersister: Persister.IPersist<Models.PositionReport>,
        private _mdBroker: Interfaces.IMarketDataBroker) {
        this._posGateway.PositionUpdate.on(this.onPositionUpdate);

        this._positionPublisher.registerSnapshot(() => (this._report === null ? [] : [this._report]));
    }
}

export class ExchangeBroker implements Interfaces.IBroker {
    private _log = log("ex:broker");

    public get hasSelfTradePrevention() {
        return this._baseGateway.hasSelfTradePrevention;
    }

    makeFee(): number {
        return this._baseGateway.makeFee();
    }

    takeFee(): number {
        return this._baseGateway.takeFee();
    }

    exchange(): Models.Exchange {
        return this._baseGateway.exchange();
    }

    public get pair() {
        return this._pair;
    }

    public get minTickIncrement() {
        return this._baseGateway.minTickIncrement;
    }

    ConnectChanged = new Utils.Evt<Models.ConnectivityStatus>();
    private mdConnected = Models.ConnectivityStatus.Disconnected;
    private oeConnected = Models.ConnectivityStatus.Disconnected;
    private _connectStatus = Models.ConnectivityStatus.Disconnected;
    public onConnect = (gwType: Models.GatewayType, cs: Models.ConnectivityStatus) => {
        if (gwType === Models.GatewayType.MarketData) {
            if (this.mdConnected === cs) return;
            this.mdConnected = cs;
        }

        if (gwType === Models.GatewayType.OrderEntry) {
            if (this.oeConnected === cs) return;
            this.oeConnected = cs;
        }

        const newStatus = this.mdConnected === Models.ConnectivityStatus.Connected && this.oeConnected === Models.ConnectivityStatus.Connected
            ? Models.ConnectivityStatus.Connected
            : Models.ConnectivityStatus.Disconnected;

        this._connectStatus = newStatus;
        this.ConnectChanged.trigger(newStatus);

        this._log.info("Connection status changed :: %s :: (md: %s) (oe: %s)", Models.ConnectivityStatus[this._connectStatus],
            Models.ConnectivityStatus[this.mdConnected], Models.ConnectivityStatus[this.oeConnected]);
        this._connectivityPublisher.publish(this.connectStatus);
    };

    public get connectStatus(): Models.ConnectivityStatus {
        return this._connectStatus;
    }

    constructor(private _pair: Models.CurrencyPair,
        private _mdGateway: Interfaces.IMarketDataGateway,
        private _baseGateway: Interfaces.IExchangeDetailsGateway,
        private _oeGateway: Interfaces.IOrderEntryGateway,
        private _connectivityPublisher: Messaging.IPublish<Models.ConnectivityStatus>) {
        this._mdGateway.ConnectChanged.on(s => {
            this.onConnect(Models.GatewayType.MarketData, s);
        });

        this._oeGateway.ConnectChanged.on(s => {
            this.onConnect(Models.GatewayType.OrderEntry, s)
        });

        this._connectivityPublisher.registerSnapshot(() => [this.connectStatus]);
    }
}
