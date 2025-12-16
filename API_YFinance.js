// main.js
const fs = require('fs');


async function _API_FetchStockMeta(ticker) {
  const { crumb, cookies } = await _getYahooCrumbAndCookies();
  //console.log(crumb) ;
  //console.log(cookies) ;
  console.log(`_API_FetchQuote will fetch ${ticker}`);
  let mktData = await _fetchYahooData(ticker, crumb, cookies);
  console.log(mktData.quoteResponse.result[0]);
  let yResult = mktData.quoteResponse.result[0];
  let jsonAsset = {
    //id bigserial not null,
    symbol: yResult.symbol,
    name: yResult.symbol,
    asset_type: "STOCK",//yResult.quoteType.toLowerCase(),
    currency: yResult.currency,
    exchange: yResult.region,
    sector: "",
    isin: "",
    cusip: "",
    quoteTTM: yResult.regularMarketPrice
    //created_at timestamp with time zone null default now(),
    //constraint assets_pkey primary key (id),
    //constraint unique_symbol_exchange unique (symbol, exchange)
    //) TABLESPACE pg_default;
  }
  console.log(jsonAsset);
  return jsonAsset;
}


async function _API_FetchQuote(ticker) {
  const { crumb, cookies } = await _getYahooCrumbAndCookies();
  //console.log(crumb) ;
  //console.log(cookies) ;
  //console.log(`_API_FetchQuote will fetch ${ticker}`);
  let mktData = await _fetchYahooData(ticker, crumb, cookies);
  //console.log(mktData.quoteResponse.result[0]) ;
  let mktPrice = mktData.quoteResponse.result[0].regularMarketPrice;
  return mktPrice;
}

//let cCurrencyTicker = `${from}${to}=X` ;
async function _API_FetchExRate(from, to) {
  const { crumb, cookies } = await _getYahooCrumbAndCookies();
  //console.log(crumb) ;
  //console.log(cookies) ;
  //let cCurrencyTicker = `${from}${to}=X` ;
  let cCurrencyTicker = `${from}${to}=X`;
  let jsonQuoteRate = await _fetchYahooDataRate(cCurrencyTicker, crumb, cookies);
  //console.log(JSON.stringify(jsonQuoteRate,null,3)) ;
  let exchangeRate = jsonQuoteRate.quoteResponse.result[0].regularMarketPrice;

  //let mktData = await _fetchYahooData(ticker,crumb, cookies);
  //let mktPrice = mktData.quoteResponse.result[0].regularMarketPrice ;
  //console.log(exchangeRate) ;
  return exchangeRate;
}

async function _getYahooCrumbAndCookies() {
  const response = await fetch('https://fc.yahoo.com', {
    headers: {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'
    }
  });
  const cookies = response.headers.get('set-cookie');
  const crumbResponse = await fetch('https://query1.finance.yahoo.com/v1/test/getcrumb', {
    headers: {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
      'Cookie': cookies
    }
  });
  const crumb = await crumbResponse.text();
  return { crumb, cookies };
}

async function _fetchYahooData(ticker, crumb, cookies) {
  let url = `https://query1.finance.yahoo.com/v7/finance/quote?&symbols=${ticker},ES=F,NQ=F,RTY=F,YM=F&fields=currency,fromCurrency,toCurrency,exchangeTimezoneName,exchangeTimezoneShortName,gmtOffSetMilliseconds,regularMarketChange,regularMarketChangePercent,regularMarketPrice,regularMarketTime,preMarketChange,preMarketChangePercent,preMarketPrice,preMarketTime,postMarketChange,postMarketChangePercent,postMarketPrice,postMarketTime,extendedMarketChange,extendedMarketChangePercent,extendedMarketPrice,extendedMarketTime,overnightMarketChange,overnightMarketChangePercent,overnightMarketPrice,overnightMarketTime&formatted=false&region=US&lang=en-US`;

  //const { crumb, cookies } = await getYahooCrumbAndCookies();
  const response = await fetch(`${url}&crumb=${crumb}`, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
      'Cookie': cookies
    }
  });

  if (response.status === 429) {
    const retryAfter = response.headers.get('Retry-After') || 60;
    console.log(`Rate limit hit, retrying after ${retryAfter} seconds`);
    await new Promise(resolve => setTimeout(resolve, retryAfter * 1000));
    return _fetchYahooData(url, crumb, cookies); // Retry
  }
  if (!response.ok) throw new Error(`Network response was not ok: ${response.statusText}`);
  return response.json();
}



//'CNYHKD=X'
async function _fetchYahooDataRate(currency, crumb, cookies) {
  //console.log(currency) ;
  let url = `https://query1.finance.yahoo.com/v7/finance/quote?&symbols=${currency},ES=F,NQ=F,RTY=F,YM=F&fields=currency,fromCurrency,toCurrency,exchangeTimezoneName,exchangeTimezoneShortName,gmtOffSetMilliseconds,regularMarketChange,regularMarketChangePercent,regularMarketPrice,regularMarketTime,preMarketChange,preMarketChangePercent,preMarketPrice,preMarketTime,postMarketChange,postMarketChangePercent,postMarketPrice,postMarketTime,extendedMarketChange,extendedMarketChangePercent,extendedMarketPrice,extendedMarketTime,overnightMarketChange,overnightMarketChangePercent,overnightMarketPrice,overnightMarketTime&formatted=false&region=US&lang=en-US`;

  const response = await fetch(`${url}&crumb=${crumb}`, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
      'Cookie': cookies
    }
  });

  if (response.status === 429) {
    const retryAfter = response.headers.get('Retry-After') || 60;
    console.log(`Rate limit hit, retrying after ${retryAfter} seconds`);
    await new Promise(resolve => setTimeout(resolve, retryAfter * 1000));
    return fetchYahooDataRate(url, crumb, cookies); // Retry
  }

  if (!response.ok) throw new Error(`Network response was not ok: ${response.statusText}`);
  return response.json();
}




async function _toolMain() {

  let jsonMeta = await _API_FetchStockMeta('INPP.L');
  console.log(jsonMeta);

  /*
  let from = "CNY" ;
  let to = "HKD" ;
  let cCurrencyTicker = `${from}${to}=X` ;
  let ex = await _API_FetchExRate(cCurrencyTicker) ;
  console.log(ex)  ;
  */
}

//_toolMain();


exports.API_FetchStockMeta = _API_FetchStockMeta;
exports.API_FetchQuote = _API_FetchQuote;
exports.API_FetchExRate = _API_FetchExRate;

