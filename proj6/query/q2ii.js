// Task 2ii

db.movies_metadata.aggregate([
    // split tagline into tokens
    {$project: {
        tokens: {
            $split: ["$tagline", " "]
        }
    }},

    // unwind tokens
    {$unwind: "$tokens"},

    // trim token and turn it to lowercase
    {$project: {
        token: {
            $trim: {
                input: {$toLower: "$tokens"},
                chars: ",.!?"
            }
        }
    }},

    // compute token length
    {$project: {
        token: 1,
        length: {$strLenCP: "$token"}
    }},

    // filter token length > 3
    {$match: {
        length: {$gt: 3}
    }},

    // group to get count
    {$group: {
        _id: "$token",
        count: {$sum: 1}
    }},

    // sort and limit
    {$sort: {count: -1}},
    {$limit: 20}
]);
