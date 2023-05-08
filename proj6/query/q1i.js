// Task 1i

db.keywords.aggregate([
    {$match: 
        {keywords: 
            {$elemMatch: 
                {$or: [
                    {name: "mickey mouse"}, 
                    {name: "marvel comic"}
                ]}
            }
        }
    },
    {$project: {movieId: 1, _id: 0}},
    {$sort: {movieId: 1}}
]);
