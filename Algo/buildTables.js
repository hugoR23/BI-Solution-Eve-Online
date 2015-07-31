/*
 * Loading libraries
 */
var mysql = require("mysql");
var csv = require("csv");
var fs = require("fs");
var async = require("async");

/*
 * Global variables
 */
var walletTransPath = "WalletTransactions.csv";
var stockInitPath = "stock_initial.csv";
var TREASURY_INIT = 1000000000;
var FIRST_DAY = new Date(2010, 06, 17, 0, 0);
var TRANS_COM = 0.005;

/*
 * Init SQL connection
 */
var SQL = mysql.createConnection({
    host: "mysql-tp.svc.enst-bretagne.fr",
    user: "hrobella",
    password: "0354142",
    database: "HROBELLA",
    //Needed to connect to school Database
    insecureAuth: true,
    multipleStatements: true
});

/*
 * Main Work
 */
createTables(function(err){
    initTransaction(walletTransPath, function(err, transactions) {
        initStock(stockInitPath, transactions, function(err, stocks) {
            console.log("Computing Indicators...")
            var stocksToSave = fillInStock(stocks, transactions);
            var treasuryToSave = fillInTreasury(transactions, TREASURY_INIT);
            saveToDB(stocksToSave, treasuryToSave, transactions);
        });
    });
});


/**
 * Used to fill in the stocks for each date and each product
 * @param  {Object} stocks  list of the stock by product
 * @param  {<Transaction>} trans  List of all the transaction
 * @return {<Stock>}        list of the stock filled
 */
function fillInStock(stocks, trans) {
    var stocksToSave = [];
    for (var product in stocks) {
        var stock = stocks[product]
        var lastStock = stock[stock.length - 1];
        var maxStockDate = trans[trans.length - 1].date;
        while (lastStock.date <= maxStockDate) {
            //Increment the date
            var newStockDate = new Date(lastStock.date);
            newStockDate.setDate(newStockDate.getDate() + 1);
            //Create the new Stock
            var curStock = new Stock(product, newStockDate);
            curStock.inherit(lastStock);
            //Go through each transactions to see which one is acting on this stock
            for (var i = 0; i < trans.length; i++) {
                if (trans[i].date < newStockDate) {
                    if (trans[i].type == product && trans[i].date > lastStock.date) {
                        curStock.update(trans[i]);
                    }
                } else {
                    break;
                }
            };
            stock.push(curStock);
            lastStock = curStock;
        }
        stocksToSave = stocksToSave.concat(stock);
    }
    return stocksToSave;
}

/**
 * Use to create one Treasury per day and update it according to the transactions
 * @param  {<Transaction>} trans     list of all the transactions
 * @param  {float} treasuryInit      Initial Value of the treasury
 * @return {<Treasury>}              List of all the treasuries created
 */
function fillInTreasury(trans, treasuryInit) {
    var initDate = FIRST_DAY;
    var lastTreas = new Treasury(initDate, treasuryInit)
    var treasuries = [lastTreas];
    var maxDate = trans[trans.length - 1].date;
    var i = 0;
    while (lastTreas.date <= maxDate) {
        var newDate = new Date(lastTreas.date);
        newDate.setDate(newDate.getDate() + 1);
        curTreas = new Treasury(newDate, lastTreas.value);
        while (i < trans.length && trans[i].date <= newDate) {
            if (trans[i].transactionType == "Buy") {
                curTreas.buy(trans[i])
            } else {
                curTreas.sell(trans[i])
            }
            i++;
        }
        lastTreas = curTreas;
        treasuries.push(curTreas);
    };
    return treasuries;
}

/**
 * Save all everything to the database
 * @param  {<Stock>} stocks list of stock to save
 * @param  {<Treasury>} Treasury list to save
 * @param  {<Object>} list of transactions to save
 */
function saveToDB(stocks, treasuries, transactions) {
    console.log("Saving to Database...");
    var objToSave = stocks.concat(treasuries);
    async.each(objToSave, function(obj, callback) {
        obj.save(SQL, callback);
    }, function(err) {
        async.each(transactions, function(trans, callback){
            var sqlDate = (trans.date.toISOString()).split("T")[0];
            SQL.query("INSERT IGNORE INTO `algos_transaction` (`date`, `transID`, `quantity`, `type`, `price`, `clientName`, `stationName`, `transactionType`, `transactionFor`) \
                        VALUES('"+sqlDate+"', "+trans.transID+", "+trans.quantity+", '"+trans.type.toString().replace(/'/g,"\\'")+"', "+trans.price+", '"+trans.clientName.toString().replace(/'/g,"\\'")+"', '"+trans.stationName.toString().replace(/'/g,"\\'")+"', '"+trans.transactionType+"', '"+trans.transactionFor+"');", function(err){
                if(err) {throw err;};
                callback();
            });
        }, function(err){
            console.log(objToSave.length + " entries");
            SQL.end();            
        })
    });
}

/**
 * Create the MySQL tables if they don't exist
 * @param  {Function} callback [description]
 */
function createTables(callback){
    /*
     * Query to generate the table algo_daily_state & algo_treasury
     */
    var dailyStateQuery = "CREATE TABLE IF NOT EXISTS `algos_daily_state` ( \
      id int(11) NOT NULL auto_increment,\
      `date` date default NULL,\
      `product` varchar(45) default NULL,\
      `stock_quantity` int(11) default NULL,\
      `stock_value` double(53,2) default NULL,\
      `margin` double(53,2) default NULL,\
      `yield` double(53,2) default NULL,\
      PRIMARY KEY  (`id`),\
      UNIQUE KEY `date_product` (`date`,`product`),\
      KEY `date_2` (`date`),\
      KEY `product` (`product`)\
    ) ENGINE=MyISAM  DEFAULT CHARSET=utf8;";
    var treasuryQuery = "CREATE TABLE IF NOT EXISTS `algos_treasury` (\
      id int(11) NOT NULL auto_increment,\
      `date` date NOT NULL,\
      `treasury` double(53,2) default NULL,\
      PRIMARY KEY  (`id`),\
      UNIQUE KEY `date` (`date`)\
    ) ENGINE=MyISAM  DEFAULT CHARSET=utf8;";
    var transactionQuery = "CREATE TABLE IF NOT EXISTS `algos_transaction` (\
      `date` datetime default NULL,\
      `transID` int(10) NOT NULL default '0',\
      `quantity` int(10) default NULL,\
      `type` varchar(42) default NULL,\
      `price` double(53,2) default NULL,\
      `clientName` varchar(22) default NULL,\
      `stationName` varchar(46) default NULL,\
      `transactionType` varchar(4) default NULL,\
      `transactionFor` varchar(8) default NULL,\
      PRIMARY KEY  (`transID`),\
      KEY `date` (`date`)\
    ) ENGINE=MyISAM DEFAULT CHARSET=utf8;\
     TRUNCATE TABLE `algos_transaction`;";

    SQL.query(dailyStateQuery, function(err) {
        if (err) throw err;
        SQL.query(treasuryQuery, function(err) {
            if (err) throw err;
            SQL.query(transactionQuery, function(err) {
                if (err) throw err;
                callback();
            });
        });
    });
}
/**
 * Initialize the Stock: get the data from the csv, create the Stock objects
 * @param  {String}   stockInitPath [description]
 * @param  {Function} callback      [description]
 */
function initStock(stockInitPath, trans, callback) {
    console.log("Reading Initial Stocks");
    fs.readFile(stockInitPath, "utf8", function(err, data) {
        if (err) throw err;
        csv.parse(data, {
            delimiter: ";",
            columns: true
        }, function(err, data) {
            csv.transform(data, function(data) {
                data.date = getDateFromStr(data.date);
                data.price = parseFloat(data.price);
                data.quantity = parseInt(data.quantity, "10");
                return data;
            }, function(err, stocks) {
                var stocksObj = {};
                var initDate = FIRST_DAY;
                for (var i = stocks.length - 1; i >= 0; i--) {
                    if (!stocksObj[stocks[i].type])
                        stocksObj[stocks[i].type] = [];
                    stocksObj[stocks[i].type].push(new Stock(stocks[i].type, initDate, stocks[i].quantity, stocks[i].price));
                }
                for (var i = 0; i < trans.length; i++) {
                    if (!stocksObj[trans[i].type]) {
                        stocksObj[trans[i].type] = [new Stock(trans[i].type, initDate)];
                    }

                }
                //console.log(stocksObj);
                callback(err, stocksObj);
            });
        });
    });
}

/**
 * Initialize the transaction list : get the data from the csv, order by date from oldest to newest
 * @param  {String}   walletTransPath Path to Csv file containing the transaction
 * @param  {Function} callback
 */
function initTransaction(walletTransPath, callback) {
    console.log("Reading Transactions");
    fs.readFile(walletTransPath, "utf8", function(err, data) {
        if (err) throw err;
        csv.parse(data, {
            columns: true,
            auto_parse: true
        }, function(err, data) {
            csv.transform(data, function(data) {
                data.date = new Date(data.date);
                //console.log(data.price)
                data.price = parseFloat(data.price);
                //console.log(data.price)
                data.quantity = parseInt(data.quantity, "10");
                return data;
            }, function(err, transactions) {
                transactions.sort(compareAtt("date", 1));
                callback(err, transactions);
            });

        });
    });
}



/*
 ********************* Stock Class ***********************
 */
/**
 * Snapshot of the stock at one date for one product
 * @param {String} product  Product concerned by the stock
 * @param {Date or String} date     Date of the snapshot
 * @param {int} quantity Quantity of unit initially in the stock
 * @param {Float} unitprice    unit price of the stock
 */
function Stock(product, date, quantity, unitprice) {
    this.product = product;
    this.date = typeof date == "string" ? getDateFromStr(date) : date;
    this.quantity = !isNaN(quantity) ? quantity : 0;
    this.price = !isNaN(unitprice) ? unitprice * quantity : 0;
    //Margin created that day for this product buy the sale
    this.margin = 0;
    this.nbSale = 0;
    this.yieldSum = 0;
    this.lastYield = 0;
    this.queue = [];
    if (quantity > 0 && unitprice > 0) {
        this.queue.push({
            "quantity": quantity,
            "unitprice": unitprice
        })
    }
}

/**
 * Add product to the Stock
 * @param {Int} quantity Nb of unit bought
 * @param {Float} unitprice    Buy Price of one unit
 */
Stock.prototype.add = function(quantity, unitprice) {
    this.quantity += quantity;
    this.price += unitprice * quantity;
    this.queue.push({
        "quantity": quantity,
        "unitprice": unitprice
    });
};

/**
 * Used to remove the product from the stock during a sale. It update the stock value, the margin and the quantity.
 * @param  {Int} quantity  Nb of unit sold
 * @param  {Float} sellPrice Price of one unit
 */
Stock.prototype.remove = function(quantity, sellPrice) {
    if (this.queue.length == 0) {
        throw new Error("Can't sell from an empty stock");
    }
    var toRemove = quantity;
    var totalBuyPrice = 0;
    while (toRemove > 0) {
        var firstin = this.queue[0];
        var removed = Math.min(toRemove, firstin.quantity)

        toRemove -= removed;
        this.queue[0].quantity -= removed;

        if (this.queue[0].quantity == 0) {
            this.queue.shift()
        }
        totalBuyPrice += removed * firstin.unitprice;
    }
    this.quantity -= quantity;
    this.price -= totalBuyPrice;
    var currentMargin = sellPrice * quantity - totalBuyPrice - TRANS_COM*totalBuyPrice;
    this.margin += currentMargin;
    this.yieldSum += currentMargin/totalBuyPrice;
    this.nbSale++;
};

/**
 * Update the stock when a transaction is done, remove or add depending on transactionType
 * @param  {Transaction} trans Transaction initiating the Stock update
 */
Stock.prototype.update = function(trans) {
    if (trans.transactionType == "Sell") {
        this.remove(trans.quantity, trans.price);
    } else {
        this.add(trans.quantity, trans.price);
    }
    //this.price = Math.round(Math.max(this.price, 0) * 1000) / 1000;
    if (this.quantity == 0) {
        this.price = 0;
    }
}

/**
 * Makes the current stock inherit attributes from another
 * @param  {Stock} stock Stock from which to inherit
 */
Stock.prototype.inherit = function(stock) {
    this.queue = stock.queue;
    this.quantity = stock.quantity;
    this.price = stock.price;
    this.lastYield = stock.getYield();
}

/**
 * Return the average yield for the day
 * @return {float} yield
 */
Stock.prototype.getYield = function() {
    return this.nbSale==0 ? this.lastYield : this.yieldSum/this.nbSale;
};

/**
 * Save the daily state to the DB
 * @param  {SQL}   SQL      SQL connexion
 * @param  {Function} callback
 */
Stock.prototype.save = function(SQL, callback) {
    var sqlDate = (this.date.toISOString()).split("T")[0];

    SQL.query("INSERT INTO algos_daily_state (product, date, stock_quantity, stock_value, margin, yield) " +
        "VALUES('" + this.product + "','" + sqlDate + "'," + this.quantity + "," + this.price + "," + this.margin + ","+this.getYield()+")" +
        "ON DUPLICATE KEY UPDATE stock_quantity=" + this.quantity + ", stock_value=" + this.price + ", margin=" + this.margin + ", yield="+this.getYield()+";", function(err) {
            if (err) throw err;
            callback();
        });
}

/*
 ********************* Treasury Class ***********************
 */

/**
 * Treasury at one date
 * @param {Date or String} date
 * @param {float} value Value of the treasury
 */
function Treasury(date, value) {
    this.date = typeof date == "string" ? getDateFromStr(date) : date;
    this.value = value;
}

/**
 * Update the Treasury after a "buy" transaction
 * @param  {Transaction} trans transaction
 */
Treasury.prototype.buy = function(trans) {
    this.value -= trans.quantity * trans.price * (1 + TRANS_COM);
};

/**
 * Update the Treasury after a "sale" transaction
 * @param  {Transaction} trans transaction
 */
Treasury.prototype.sell = function(trans) {
    this.value += trans.quantity * trans.price * (1 - TRANS_COM);
};

/**
 * Save the daily state to the DB
 * @param  {SQL}   SQL      SQL connexion
 * @param  {Function} callback
 */
Treasury.prototype.save = function(SQL, callback) {
    var sqlDate = (this.date.toISOString()).split("T")[0];

    SQL.query("INSERT INTO algos_treasury (date, treasury) " +
        "VALUES('" + sqlDate + "'," + this.value + ")" +
        "ON DUPLICATE KEY UPDATE treasury=" + this.value + ";", function(err) {
            if (err) throw err;
            callback();
        });
}
/*
 ***************** UTILITIES FUNCTIONS ********************
 */

/**
 * Comparison function between two object using one attribute as comparator
 * @param  {String} attribute attribute used for the comparison
 * @param  {int} order     Sorting Order
 * @return {Function}         Comparaison function
 */
function compareAtt(attribute, order) {
    return function(t1, t2) {
        if (t1[attribute] < t2[attribute])
            return -1 * order;
        if (t1[attribute] > t2[attribute])
            return 1 * order;
        return 0;
    }
};

/**
 * Convert a String to a date object
 * @param  {String} str String of a date under this format: 17/10/2014
 * @return {Date}     Date Object
 */
function getDateFromStr(str) {
    var dateStr = str.split("/");
    var date = new Date();
    date.setDate(dateStr[0]);
    date.setMonth(dateStr[1] - 1);
    date.setFullYear(dateStr[2]);
    date.setHours(0, 0, 0);
    return date;
}

