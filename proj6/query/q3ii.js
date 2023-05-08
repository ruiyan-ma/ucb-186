// Task 3ii

db.credits.aggregate([
    // match all documents where Wes Anderson is the director
    {$match: {
        crew: {
            $elemMatch: {
                job: "Director",
                id: 5655
            }
        }
    }},

    // unwind cast
    {$unwind: "$cast"},

    // group by cast id and name
    {$group: {
        _id: {
            id: "$cast.id", 
            name: "$cast.name"
        },
        count: {$sum: 1}
    }},

    // project name, id, count
    {$project: {
        _id: 0,
        count: 1,
        id: "$_id.id",
        name: "$_id.name"
    }},

    // sort and limit
    {$sort: {
        count: -1,
        id: 1
    }},
    {$limit: 5}
]);
