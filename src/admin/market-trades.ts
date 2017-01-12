/// <reference path='../common/models.ts' />
/// <reference path='../common/messaging.ts' />
/// <reference path='shared_directives.ts'/>

import {NgZone, Component, Inject, OnInit, OnDestroy} from '@angular/core';
import {GridOptions} from "ag-grid/main";
import moment = require('moment');

import Models = require('../common/models');
import Messaging = require('../common/messaging');
import {SubscriberFactory} from './shared_directives';

class DisplayMarketTrade {
  price: number;
  size: number;
  time: moment.Moment;
  recent: boolean;

  qA: number;
  qB: number;
  qAz: number;
  qBz: number;

  mA: number;
  mB: number;
  mAz: number;
  mBz: number;

  make_side: string;

  constructor(
    public trade: Models.MarketTrade
  ) {
    this.price = DisplayMarketTrade.round(trade.price);
    this.size = DisplayMarketTrade.round(trade.size);
    this.time = (moment.isMoment(trade.time) ? trade.time : moment(trade.time));

    if (trade.quote != null) {
      if (trade.quote.ask !== null) {
        this.qA = DisplayMarketTrade.round(trade.quote.ask.price);
        this.qAz = DisplayMarketTrade.round(trade.quote.ask.size);
      }

      if (trade.quote.bid !== null) {
        this.qB = DisplayMarketTrade.round(trade.quote.bid.price);
        this.qBz = DisplayMarketTrade.round(trade.quote.bid.size);
      }
    }

    if (trade.ask != null) {
      this.mA = DisplayMarketTrade.round(trade.ask.price);
      this.mAz = DisplayMarketTrade.round(trade.ask.size);
    }

    if (trade.bid != null) {
      this.mB = DisplayMarketTrade.round(trade.bid.price);
      this.mBz = DisplayMarketTrade.round(trade.bid.size);
    }

    this.make_side = Models.Side[trade.make_side];

    this.recent = true;
  }

  private static round(num: number) {
    return Math.round(num * 100) / 100;
  }
}

@Component({
  selector: 'market-trades',
  template: `<ag-grid-ng2 style="height: 180px;" #agGrid class="ag-material" [gridOptions]="gridOptions"></ag-grid-ng2>`
})
export class MarketTradesComponent implements OnInit, OnDestroy {

  private gridOptions: GridOptions = {};

  private subscriberMarketTrade: Messaging.ISubscribe<Models.MarketTrade>;

  constructor(
    @Inject(NgZone) private zone: NgZone,
    @Inject(SubscriberFactory) private subscriberFactory: SubscriberFactory
  ) {}
      // class="table table-striped table-hover table-condensed"
      // showGroupPanel: false,
      // rowHeight: 20,
      // headerRowHeight: 20,
      // groupsCollapsedByDefault: true,
      // enableColumnResize: true,

  ngOnInit() {
    this.gridOptions.rowData = [];
    this.gridOptions.columnDefs = this.createColumnDefs();

    this.subscriberMarketTrade = this.subscriberFactory.getSubscriber(this.zone, Messaging.Topics.MarketTrade)
      .registerSubscriber(this.addRowData, x => x.forEach(this.addRowData))
      .registerDisconnectedHandler(() => this.gridOptions.rowData.length = 0);
  }

  ngOnDestroy() {
    this.subscriberMarketTrade.disconnect();
  }

  private createColumnDefs = () => {
    return [
        { width: 90, field: 'time', headerName: 'time', cellFilter: "momentShortDate",
          comparator: (a: moment.Moment, b: moment.Moment) => a.diff(b),
          sort: 'desc', cellClass: (params) => {
            return 'fs11px '+(!params.data.recent ? "text-muted" : "");
        } },
        { width: 60, field: 'price', headerName: 'px', cellClass: (params) => {
            return (params.data.make_side === 'Ask') ? "sell" : "buy";
        } },
        { width: 50, field: 'size', headerName: 'sz', cellClass: (params) => {
            return (params.data.make_side === 'Ask') ? "sell" : "buy";
        } },
        { width: 40, field: 'make_side', headerName: 'ms' , cellClass: (params) => {
          if (params.value === 'Bid') return 'buy';
          else if (params.value === 'Ask') return "sell";
        }},
        { width: 40, field: 'qBz', headerName: 'qBz' },
        { width: 50, field: 'qB', headerName: 'qB' },
        { width: 50, field: 'qA', headerName: 'qA' },
        { width: 40, field: 'qAz', headerName: 'qAz' },
        { width: 40, field: 'mBz', headerName: 'mBz' },
        { width: 50, field: 'mB', headerName: 'mB' },
        { width: 50, field: 'mA', headerName: 'mA' },
        { width: 40, field: 'mAz', headerName: 'mAz' }
    ];
  }

  private addRowData = (u: Models.MarketTrade) => {
    if (u != null)
      this.gridOptions.rowData.push(new DisplayMarketTrade(u));

    for(var i=this.gridOptions.rowData.length-1;i>-1;i--)
      if (Math.abs(moment.utc().valueOf() - this.gridOptions.rowData[i].time.valueOf()) > 3600000)
        this.gridOptions.rowData.splice(i,1);
      else if (Math.abs(moment.utc().valueOf() - this.gridOptions.rowData[i].time.valueOf()) > 7000)
        this.gridOptions.rowData[i].recent = false;
  }
}
