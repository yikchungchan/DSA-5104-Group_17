// MongoDB Queries with Default Sturctures
// Run the queries on reduced data collections first before running on the whole dataset.
// Assume we only want "asin", "title" and "description" for the recommendation system.

// Query 1: Recommend items based on total sales
db.transaction_data.aggregate([
    {
      // Find asin for the required items
      $group: {
        _id: "$asin", 
        totalSales: { $sum: "$copy" } 
      }
    },
    { $sort: { totalSales: -1 } },
    { $limit: 10 }, 
    {
      // Look-up the product information in "meat_data"
      $lookup: {
        from: "meta_data", 
        localField: "_id",
        foreignField: "asin",
        as: "product_info"
      }
    },
    {
      $unwind: "$product_info"
    },
    {
        $project: {
          _id: 0,
          totalSales: 1,
          product_info: {
            asin: "$_id",
            title: "$product_info.title",
            description: "$product_info.description"
          }
        }
      }
    ]);
  
  


// Query 2: Recommend items based on overall rating
db.review_data.aggregate([
    {
      $group: {
        _id: "$asin", // Group by the ASIN to calculate average rating
        averageRating: { $avg: "$overall" } // Calculate the average rating
      }
    },
    { $sort: { averageRating: -1 } }, // Sort by average rating in descending order
    { $limit: 10 }, // Limit to top 10 recommended items
    {
      $lookup: {
        from: "meta_data", 
        localField: "_id",
        foreignField: "asin",
        as: "product_info"
      }
    },
    {
      $unwind: "$product_info" // Flatten the array of joined product info
    },
    {
        $project: {
          _id: 0,
          averageRating: 1,
          product_info: {
            asin: "$_id",
            title: "$product_info.title",
            description: "$product_info.description"
          }
        }
      }
  ]);
  


// Query 3: Recommend items based on "Also_buy"
var targetUserID = "A2G6MU2JV29206"; // Sample Reviewer ID
var targetUserID = "A1P4HSLDHS980L"; 

// Step 1: Return the "also_buy" list
var alsoBuyList = db.transaction_data_sample.aggregate([
    {
      $match: {
        reviewerID: targetUserID
      }
    },
    {
      $lookup: {
        from: "meta_data_sample",
        localField: "asin",
        foreignField: "asin",
        as: "product_info"
      }
    },
    {
      $unwind: "$product_info"
    },
    {
      $group: {
        _id: "$reviewerID",
        also_buy: { $push: "$product_info.also_buy" }
      }
    },
    {
      $project: {
        _id: 0,
        also_buy: { $reduce: { input: "$also_buy", initialValue: [], in: { $concatArrays: ["$$value", "$$this"] } } }
      }
    },
    { $unwind: "$also_buy" }, // Unwind the nested arrays
    { $unwind: "$also_buy" } // You might need to unwind again if the items in also_buy are arrays themselves.
  ]).toArray(function(err, result) {
    if (err) throw err;
    printjson(result);
  });

// Step 2: Return the detailed info
var selectedASINs = alsoBuyList.map(a => a.also_buy) .sort(() => 0.5 - Math.random()) .slice(0, 10); // Get the first 10 items

db.meta_data.aggregate([  // Here, look up in the whole meta_data database
  {
    $match: {
      asin: { $in: selectedASINs }
    }
  },
  {
    $project: {
      _id: 0,
      product_info: {
        asin: "$asin",
        title: "$title",
        description: "$description",
      }
    }
  }
]).toArray(function(err, result) {
  if (err) throw err;
  printjson(result);
});


// or use this one query (on whole dataset)
db.transaction_data.aggregate([
    {
      $match: {
        reviewerID: targetUserID
      }
    },
    {
      $lookup: {
        from: "meta_data",
        localField: "asin",
        foreignField: "asin",
        as: "product_info"
      }
    },
    {
      $unwind: "$product_info"
    },
    {
      $group: {
        _id: "$reviewerID",
        also_buy: { $push: "$product_info.also_buy" }
      }
    },
    {
      $project: {
        also_buy: { $reduce: { input: "$also_buy", initialValue: [], in: { $concatArrays: ["$$value", "$$this"] } } }
      }
    },
    { $unwind: "$also_buy" },
    { $unwind: "$also_buy" },

    // Add a random field to each document.
    {
      $addFields: {
        randomField: { $rand: {} }
      }
    },
    {
      $sort: {
        randomField: 1
      }
    },
    {
      $limit: 6
    },
    // Look up the detailed information from the metadata collection.
    {
      $lookup: {
        from: "meta_data",
        localField: "also_buy",
        foreignField: "asin",
        as: "product_details"
      }
    },
    {
      $unwind: "$product_details"
    },

    // Project the required fields.
    {
      $project: {
        _id: 0,
        product_info: {
          asin: "$also_buy",
          title: "$product_details.title",
          description: "$product_details.description",
        }
      }
    }
  ]).toArray(function(err, result) {
    if (err) throw err;
    printjson(result);
});



// Query on test sample:
db.transaction_data_sample.aggregate([
  {
    $match: {
      reviewerID: targetUserID
    }
  },
  {
    $lookup: {
      from: "meta_data_sample",
      localField: "asin",
      foreignField: "asin",
      as: "product_info"
    }
  },
  {
    $unwind: "$product_info"
  },
  {
    $group: {
      _id: "$reviewerID",
      also_buy: { $push: "$product_info.also_buy" }
    }
  },
  {
    $project: {
      also_buy: { $reduce: { input: "$also_buy", initialValue: [], in: { $concatArrays: ["$$value", "$$this"] } } }
    }
  },
  { $unwind: "$also_buy" },
  { $unwind: "$also_buy" },

  // Add a random field to each document.
  {
    $addFields: {
      randomField: { $rand: {} }
    }
  },
  {
    $sort: {
      randomField: 1
    }
  },
  {
    $limit: 6
  },
  // Look up the detailed information from the metadata collection.
  {
    $lookup: {
      from: "meta_data",
      localField: "also_buy",
      foreignField: "asin",
      as: "product_details"
    }
  },
  {
    $unwind: "$product_details"
  },

  // Project the required fields.
  {
    $project: {
      _id: 0,
      product_info: {
        asin: "$also_buy",
        title: "$product_details.title",
        description: "$product_details.description",
      }
    }
  }
]).toArray(function(err, result) {
  if (err) throw err;
  printjson(result);
}); 




// Query 4: Recommend items based on "Also_view" (similar to query 3)
db.transaction_data.aggregate([
    {
      $match: {
        reviewerID: targetUserID 
      }
    },
    {
      $lookup: {
        from: "meta_data",
        localField: "asin",
        foreignField: "asin",
        as: "product_info"
      }
    },
    {
      $unwind: "$product_info"
    },
    {
      $group: {
        _id: "$reviewerID",
        also_view: { $push: "$product_info.also_view" }
      }
    },
    {
      $project: {
        also_view: { $reduce: { input: "$also_view", initialValue: [], in: { $concatArrays: ["$$value", "$$this"] } } }
      }
    },
    { $unwind: "$also_view" },
    { $unwind: "$also_view" },
    
    // Add a random field to each document for random selection.
    {
      $addFields: {
        randomField: { $rand: {} }
      }
    },
    {
      $sort: {
        randomField: 1
      }
    },
    {
      $limit: 10 
    },
    
    // Look up the detailed information from the metadata collection.
    {
      $lookup: {
        from: "meta_data",
        localField: "also_view",
        foreignField: "asin",
        as: "product_details"
      }
    },
    {
      $unwind: "$product_details"
    },
  
    // Project the required fields.
    {
      $project: {
        _id: 0,
        product_info: {
          asin: "$also_view",
          title: "$product_details.title",
          description: "$product_details.description",
        }
      }
    }
  ]).toArray(function(err, result) {
    if (err) throw err;
    printjson(result);
  });

  
// Query 5 : Top-10 Selling List
db.transaction_data.aggregate([
    {
      $group: {
        _id: "$asin", 
        totalSales: { $sum: 1 } // Count the number of sales for each product
      }
    },
    {
      $sort: { totalSales: -1 } // Sort by total sales in descending order
    },
    {
      $limit: 10 // Limit to top 10
    },
    {
      $lookup: {
        from: "meta_data",
        localField: "_id",
        foreignField: "asin",
        as: "product_info"
      }
    },
    {
      $unwind: "$product_info" 
    },
    {
      $project: {
        _id: 0,
        asin: "$_id",
        totalSales: 1,
        title: "$product_info.title",
        description: "$product_info.description"
      }
    }
  ]).toArray();
  

// Query 6: Top-10 Like List
db.review_data.aggregate([
    {
      $group: {
        _id: "$asin",
        averageRating: { $avg: "$overall" } 
      }
    },
    {
      $sort: { averageRating: -1 } // Sort by averageRating in descending order
    },
    {
      $limit: 10 // Limit to top 10
    },
    {
      $lookup: {
        from: "meta_data",
        localField: "_id",
        foreignField: "asin",
        as: "product_info"
      }
    },
    {
      $unwind: "$product_info" 
    },
    {
      $project: {
        _id: 0,
        asin: "$_id",
        averageRating: 1,
        title: "$product_info.title",
        description: "$product_info.description"
      }
    }
  ]).toArray(function(err, result) {
    if (err) throw err;
    printjson(result);
  });


// Query 7: Influential Users:
db.user_data.aggregate([
    {
      $project: {
        reviewerID: 1,
        reviewerName: 1,
        totalVotes: 1
      }
    },
    {
      $sort: {
        totalVotes: -1 // Based on TotalVotes
      }
    },
    {
      $limit: 10 
    }
  ]).toArray();

db.user_data.aggregate([
    {
      $project: {
        reviewerID: 1,
        reviewerName: 1,
        fans: 1
      }
    },
    {
      $sort: {
        fans: -1 // Based on fans 
      }
    },
    {
      $limit: 10 
    }
  ]).toArray();
  
  
// Query 8: Retrieve Transaction records among selected time  
// For example, from 2012-01-01 to 2012-01-05
var startDate = new Date("2012-01-01").getTime();
var endDate = new Date("2012-01-05").getTime();

db.transaction_data.find({
  "transactionTime": {
    $gte: startDate,
    $lte: endDate
  }
}).toArray();