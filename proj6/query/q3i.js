// Task 3i

db.credits.aggregate([
    // unwind cast
    {$unwind: "$cast"},
    
    // find all movies with Stan Lee
    {$match: {"cast.id": 7624}},

    // look up in movies_metadata
    {$lookup: {
        from: "movies_metadata",
        localField: "movieId",
        foreignField: "movieId",
        as: "movies"
    }},

    // project title, release_date and character
    {$project: {
        _id: 0,
        title: {$first: "$movies.title"},
        release_date: {$first: "$movies.release_date"},
        character: "$cast.character"
    }},

    // sort by release_date
    {$sort: {release_date: -1}}
]);
