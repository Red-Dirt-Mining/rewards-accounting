const fs = require('fs');
const csv = require('csv-parser');
const moment = require('moment-timezone');
const fetch = require('isomorphic-fetch');

// Define input and output file paths
const inputFile = 'btc_rewards_score.csv';
const outputFile = 'dailyRewards.csv';

// Define timezones
const utcTimezone = 'UTC';
const centralTimezone = 'America/Chicago';

// Create an object to store daily rewards
const dailyRewards = {};

// Create an object to store daily price
const dailyPrice = {};

const formatDate = (date) => {
  // convert YYYY-MM-DD to DD-MM-YYYY
  const dateParts = date.split('-');
  return `${dateParts[2]}-${dateParts[1]}-${dateParts[0]}`;
}

const getPriceOnDate = async (date) => {
  const response = await fetch(`https://api.coingecko.com/api/v3/coins/bitcoin/history?date=${date}&localization=false&id=bitcoin`)
  .catch(err => {
    console.error(err, {message: err.message})
  })
  console.log('Getting price for date', {date, responseStatus: response.status})
  const data = await response.json()
  return data.market_data.current_price.usd
}

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// Read and process the CSV file
(async () => {
  const readStream = fs.createReadStream(inputFile)
  for await (const row of readStream.pipe(csv())) {
    const foundAt = row.found_at;
    const userReward = parseFloat(row.user_reward);

    // Convert UTC to Central time
    const centralDateTime = moment.tz(foundAt, utcTimezone).tz(centralTimezone);
    const centralDate = centralDateTime.format('YYYY-MM-DD');

    // Add user_reward to the daily total
    if (!dailyRewards[centralDate]) {
      dailyRewards[centralDate] = 0;
    }
    dailyRewards[centralDate] += userReward;

    // Add price to the row
    if (!dailyPrice[centralDate]) {
      /*
        CoinGecko's Public API has a rate limit of 10-30 calls/minute, and doesn't come with an API key.
        If you need something more powerful, subscribe to one of their paid API Plans and use a Pro API key.
      */
      await delay(6000); // Delay for 6 seconds to meet free API limit (adjust as needed)
      dailyPrice[centralDate] = await getPriceOnDate(formatDate(centralDate));
    }
  }

  const outputData = [];
  for (const date in dailyRewards) {
    outputData.push({
      date,
      total_reward: dailyRewards[date].toFixed(8), // Round to sats
      price: dailyPrice[date],
      value: (dailyRewards[date] * dailyPrice[date]).toFixed(2), // Round to cents
    });
  }

  const csvHeader = ['date', 'total_reward', 'price', 'value'];
  const csvRows = outputData.map((row) => csvHeader.map((header) => row[header]).join(','));

  const csvContent = [csvHeader.join(','), ...csvRows].join('\n');

  fs.writeFileSync(outputFile, csvContent);

  console.log('CSV processing completed.');
})();
