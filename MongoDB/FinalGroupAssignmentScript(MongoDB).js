use BC2402_GroupProject

db.country_vaccinations.findOne()
db.country_vaccinations_by_manufacturer.findOne()
db.covid19data.findOne()

// Converting the columns that have values into double and dropping of the irrelevant columns. Converting all 'location' to 'country' as well
db.country_vaccinations.aggregate([
    {$project: {
        country:1,
        iso_code:1,
        "date":{$convert:{input:"$date", to: "date"}},
        "total_vaccinations":{$convert:{input:"$total_vaccinations", to: "double"}},
        "people_vaccinated":{$convert:{input:"$people_vaccinated", to: "double"}},
        "people_fully_vaccinated":{$convert:{input:"$people_fully_vaccinated", to: "double"}},
        "daily_vaccinations":{$convert:{input:"$daily_vaccinations", to: "double"}},
        "total_vaccinations_per_hundred":{$convert:{input:"$total_vaccinations_per_hundred", to: "double"}},
        "people_vaccinated_per_hundred":{$convert:{input:"$people_vaccinated_per_hundred", to: "double"}},
        "people_fully_vaccinated_per_hundred":{$convert:{input:"$people_fully_vaccinated_per_hundred", to: "double"}},
        "daily_vaccinations_per_million":{$convert:{input:"$daily_vaccinations_per_million", to: "double"}},
        vaccines:1,
        source_name:1
    }},
    {$out: "country_vac"}
])

db.country_vaccinations_by_manufacturer.aggregate([
    {$project: {
        "country": "$location",
        "date":{$convert:{input:"$date", to: "date"}},
        vaccine:1,
        "total_vaccinations":{$convert:{input:"$total_vaccinations", to: "double"}}
    }},
    {$out:"country_vac_manu"}
])

db.covid19data.aggregate([
    {$project: {
        "country": "$location",
        "date":{$convert:{input:"$date", to: "date"}},
        "total_cases":{$convert:{input:"$total_cases", to: "double"}},
        "new_cases":{$convert:{input:"$new_cases", to: "double"}},
        "new_cases_smoothed":{$convert:{input:"$new_cases_smoothed", to: "double"}},
        "total_cases_per_million":{$convert:{input:"$total_cases_per_million", to: "double"}},
        "new_cases_per_million":{$convert:{input:"$new_cases_per_million", to: "double"}},
        "population":{$convert:{input:"$population", to: "double"}},
        iso_code:1,
        continent:1,
    }},
    {$out: "covid19data2"}
])

// Testing collections
db.country_vac.findOne()
db.country_vac_manu.findOne()
db.covid19data2.find({})

// Question 1: Display a list of total vaccinations per day in Singapore
db.country_vac.aggregate([
    {$match:{country:"Singapore"}},
    {$project:{_id:0, country:1,date:1,total_vaccinations:1}},
    {$sort: {date:1}},
    ])

// Question 2: Display the sum of daily vaccinations among ASEAN countries.
db.country_vac.aggregate([
    {$match:{country:{$in:['Brunei', 'Cambodia', 'Indonesia', 'Laos', 'Malaysia', 'Myanmar', 'Philippines', 'Singapore', 'Thailand', 'Vietnam']}}},
    {$project: {_id: 0, country : 1, TotalVax: {$convert: {input: "$daily_vaccinations" , to : "double"}}}},
    {$group:{_id:{groupByCountry:"$country"},TotalVax:{$sum:"$TotalVax"}}},
    {$project:{_id:0,ASEANCountry:"$_id.groupByCountry",TotalVax:1}},
    {$sort:{TotalVax:-1}}
])

// Question 3: Identify the maximum daily vaccinations per million of each country. Sort the list based on daily vaccinations per million in a descending order.
db.country_vac.aggregate([
    {$group:{_id:{groupByCountry:"$country"}, maxDailyVac:{$max:"$daily_vaccinations_per_million"}}},
    {$project: {_id:0, country:"$_id.groupByCountry", maxDailyVac:1}},
    {$sort:{maxDailyVac:-1}}
])

// Question 4: Which is the most administrated vaccine? Display a list of total administration (i.e., sum of total vaccinations) per vaccine.
db.country_vac_manu.aggregate([
    { $group: { _id: { groupByCountry: "$country", groupByVac: "$vaccine" }, TotalVac: { $sum: "$total_vaccinations" } } },
    { $project: { _id: 0, Country: "$_id.groupByCountry", Vaccine: "$_id.groupByVac", TotalVac: 1 } },
    { $group: { _id: { groupByVac2: "$Vaccine" }, TotalAdministered: { $sum: "$TotalVac" } } },
    { $project: { _id: 0, Vaccine: "$_id.groupByVac2", TotalAdministered: 1 } },
    { $sort: { TotalAdministered: -1 } }
])

// Question 5: Italy has commenced administrating various vaccines to its populations as a vaccine becomes available. Identify the first dates of each vaccine being administrated, then compute the difference in days between the earliest date and the 4th date.
db.country_vac_manu.aggregate(
    {$match: {country:"Italy"}},
    {$group: {_id:{groupByVaccine: "$vaccine"}, firstAdminstratedDate:{$min: "$date"}}},
    {$sort: {firstAdminstratedDate:1}},
    {$project: {_id:0,Vaccine: "$_id.groupByVaccine",firstAdminstratedDate:1}},
    )

db.country_vac_manu.aggregate(
    {$match: {country:"Italy"}},
    {$group: {_id:{groupByVaccine: "$vaccine"}, date:{$min: "$date"}}},
    {$sort: {firstAdminstratedDate:1}},
    {$project: {_id:0,Vaccine: "$_id.groupByVaccine",date:1 }},
    {$limit: 4},
    {$group: { _id: null, minDate:{$max: "$date"}, maxDate:{$min: "$date"}}},
    {$project: {_id:0, Difference_In_Days: {$dateDiff: { 
        startDate: "$maxDate",
        endDate: "$minDate",
        unit: "day",
    }}}}
    )
    
// Question 6: What is the country with the most types of administrated vaccine?
db.country_vac_manu.aggregate([
    {$group:{_id:{groupByCountry: "$country", groupByVaccine: "$vaccine"}}},
    {$group:{_id:{groupByCountry2:"$_id.groupByCountry"},vaccine:{$push:"$_id.groupByVaccine"}}},
    {$project:{_id:0,"Country":"$_id.groupByCountry2","NumberOfVaccineTypes":{$size:"$vaccine"},"Vaccines":"$vaccine"}},
    {$sort:{NumberOfVaccineTypes:-1}},
    {$limit:1}
])


// Question 7: What are the countries that have fully vaccinated more than 60% of its people? For each country, display the vaccines administrated.
db.country_vac.aggregate(
    {$group: { _id: {groupByCountry: "$country", groupByVaccine:"$vaccines"} , 
        VaccinatedPercentage: {$max: "$people_fully_vaccinated_per_hundred"}}},
    {$match: {"VaccinatedPercentage": {$gt:60}}},
    {$sort: {VaccinatedPercentage: -1}},
    {$project: {_id:0, country:"$_id.groupByCountry",vaccine:"$_id.groupByVaccine",VaccinatedPercentage:1}},
    )

// Question 8: Monthly vaccination insight – display the monthly total vaccination amount of each vaccine per month in the United States.
db.country_vac_manu.aggregate([
    {$match:{country:"United States"}},
    {$group:{_id:{monthNum:{$month:"$date"},groupbyVaccine:"$vaccine"},"maxTotalVac":{$max:"$total_vaccinations"}}},
    {$group:{_id:{monthByGroup:"$_id.monthNum"},
        Insights:{$push:{"Vaccine":"$_id.groupbyVaccine","monthly_total_vaccine":"$maxTotalVac"}}}},
    {$project:{_id:0,"Month":"$_id.monthByGroup","Insights":1}},
    {$sort:{Month:1}}
])

// Question 9: Days to 50 percent. Compute the number of days (i.e., using the first available date on records of a country) that each country takes to go above the 50% threshold of vaccination administration (i.e., total_vaccinations_per_hundred > 50)
db.country_vac.aggregate([
    {$group:{
        _id:{groupbyCountry:"$country"},
        "FirstAvailableDate":{$min:"$date"},
        "DailyPercentages":{$push:{"date":"$date","VaccinationsPercentage":"$total_vaccinations_per_hundred"}}}},
    {$project:{
        _id:0,
        FirstAvailableDate:1,
        "Country":"$_id.groupbyCountry",
        DailyPercentages:{$filter:{
            input:"$DailyPercentages",
            as: "perc",
            cond: {$gt:["$$perc.VaccinationsPercentage",50]}
        }}}},
    {$project:{_id:0,Country:1,"Days_to_over_50%":{$dateDiff:{
        startDate:"$FirstAvailableDate",
        endDate:{$min:"$DailyPercentages.date"},
        unit:"day"
    }}}},
    {$match:{"Days_to_over_50%":{$ne:null}}},
    {$sort:{Country:1}}
])


// Question 10: Compute the global total of vaccinations per vaccine.
db.country_vac_manu.aggregate([
    {$group:{_id:{groupByCountry:"$country",groupByVax:"$vaccine"},TotalVax:{$max:"$total_vaccinations"}}},
    {$project:{_id:0, Country:"$_id.groupByCountry",Vaccine:"$_id.groupByVax",TotalVax:1}},
    {$group:{_id:{groupByVax2:"$Vaccine"},TotalAdministered:{$sum:"$TotalVax"}}},
    {$project:{_id:0,Vaccine:"$_id.groupByVax2","Global_Total":"$TotalAdministered"}},
    {$sort:{TotalAdministered:-1}}
])

// Question 11: What is the total population in Asia?
db.covid19data2.aggregate([
    {$match: {continent: "Asia"}},
    {$group: {_id:{groupByCountry:"$country"}, population_asia:{$max: "$population"}}},
    {$group: {_id:{continent:"Asia"},population:{$sum:"$population_asia"}}}])

// Question 12: What is the total population among the ten ASEAN countries?
db.covid19data2.aggregate([
    {$match:{country:{$in:["Singapore","Brunei", "Indonesia", "Myanmar", "Malaysia", "Laos", "Philippines", "Thailand", "Vietnam", "Cambodia"]}}},
    {$group:{_id:{groupByCountry:"$country"},"TotalPopulation":{$max:"$population"}}},
    {$group:{_id:{},"Total_ASEAN_Population":{$sum:"$TotalPopulation"}}},
    {$project:{_id:0,Total_ASEAN_Population:1}},
])

// Question 13: Generate a list of unique data sources (source_name)
db.country_vac.distinct("source_name")


// Question 14: Specific to Singapore, display the daily total_vaccinations starting (inclusive) March-1 2021 through (inclusive) May-31 2021. 
// if question asks for "total daily vaccination" -> sum(daily_vaccination) across all vaccines for each individual day. Thus "daily total_vaccination" will be the accumulated total vaccinations across all days for that specific day 
db.country_vac.aggregate([
    {
        $match: {
            $and: [{
                country: "Singapore",
                date: { $gte: ISODate("2021-03-01"), $lte: ISODate("2021-05-31") }
            }]
        }
    },
    { $project: { _id: 0, date: 1, country: 1, total_vaccinations: 1 } },
    { $sort: {date:1}}
])


// Question 15: When is the first batch of vaccinations recorded in Singapore?
db.country_vac.aggregate(
    {$match: {"country": "Singapore"}},
    {$match: {"total_vaccinations":{$gt:0}}},
    {$sort: {total_vaccinations:1}},
    {$project: {_id:0, "First_Vaccination_Date":"$date"}},
    {$limit: 1}
    )

// Question 16: Based on the date identified in (5), specific to Singapore, compute the total number of new cases thereafter. For instance, if the date identified in (5) is Jan-1 2021, the total number of new cases will be the sum of new cases starting from (inclusive) Jan-1 to the last date in the dataset.
db.covid19data2.aggregate([
    {$lookup:
        {
            from: "country_vac",
            pipeline: [
                {$match: {"country": "Singapore"}},
                {$match: {"total_vaccinations":{$gt:0}}},
                {$sort: {total_vaccinations:1}},
                {$project: {_id:0, "date":"$date"}},
                {$limit: 1}
            ],
            as: "First_vaccination_date"
        }
    },
    {$unwind: "$First_vaccination_date"},
    {$match:
        {$and:[
            {country:"Singapore"},
            {$expr:{$gte: ["$date","$First_vaccination_date.date"]}}
            ]}
    },
    {$group:{_id:{groupbycountry:"$country"},sum_newcases:{$sum:"$new_cases"}}},
    {$project:{_id:0,"Total_number_of_cases_after_first_batch_of_vaccinations":"$sum_newcases"}}
    ])


// Question 17: Compute the total number of new cases in Singapore before the date identified in (5). For instance, if the date identified in (5) is Jan-1 2021 and the first date recorded (in Singapore) in the dataset is Feb-1 2020, the total number of new cases will be the sum of new cases starting from (inclusive) Feb-1 2020 through (inclusive) Dec-31 2020.
db.covid19data2.aggregate([
    {$lookup:
        {
            from: "country_vac",
            pipeline: [
                {$match: {"country": "Singapore"}},
                {$match: {"total_vaccinations":{$gt:0}}},
                {$sort: {total_vaccinations:1}},
                {$project: {_id:0, "date":"$date"}},
                {$limit: 1}
            ],
            as: "First_vaccination_date"
        }
    },
    {$unwind: "$First_vaccination_date"},
    {$match:
        {$and:[
            {country:"Singapore"},
            {$expr:{$lt: ["$date","$First_vaccination_date.date"]}}
            ]}
    },
    {$group:{_id:{groupbycountry:"$country"},sum_newcases:{$sum:"$new_cases"}}},
    {$project:{_id:0,"Total_number_of_cases_before_first_batch_of_vaccinations":"$sum_newcases"}}
    ])


// Question 18: Herd immunity estimation. On a daily basis, specific to Germany, calculate the percentage of new cases and total vaccinations on each available vaccine in relation to its population.
db.covid19data2.aggregate([
    {$match:{country:"Germany"}},
    {$lookup:{
        from:"country_vac_manu",
        localField:"date",
        foreignField:"date",
        pipeline:[
            {$match:{country:"Germany"}},
            {$project:{country:1,date:1,vaccine:1,total_vaccinations:1}}
            ],
            as: "vac_data"
    }},
    {$unwind:"$vac_data"},
    {$project:{
        _id:0,
        country:1,
        date:1,
        "perc_new_cases":{$multiply:[{$divide:["$new_cases","$population"]},100]},
        "perc_total_vac":{$multiply:[{$divide:["$vac_data.total_vaccinations","$population"]},100]},
        "vac":"$vac_data.vaccine"
    }},
    {$group:{_id:{groupbycountry:"$country",groupbydate:"$date",groupbycases:"$perc_new_cases"},
        "VacPer2":{$push:{"Vaccine":"$vac","Percentage_total_cases":"$perc_total_vac"}}}},
    {$project:{
        _id:0,
        "Country":"$_id.groupbycountry",
        "Date":"$_id.groupbydate"
        "Percentage_new_cases":"$_id.groupbycases",
        "Vaccines_percentage":"$VacPer2"
    }},
    {$sort:{Date:1}},
])


// Question 19: Vaccination Drivers. Specific to Germany, based on each daily new case, display the total vaccinations of each available vaccines after 20 days, 30 days, and 40 days.
db.covid19data2.aggregate([
    {$match:{country:"Germany"}},
    {$project:{
        _id:0,
        country:1,
        date:1,
        new_cases:1,
        "twentydayslater":{$dateAdd: { 
            startDate:"$date" ,
            unit: "day",
            amount: 20,
        }},
        "thirtydayslater":{$dateAdd: { 
            startDate:"$date" ,
            unit: "day",
            amount: 30,
        }},
        "fortydayslater":{$dateAdd: { 
            startDate:"$date" ,
            unit: "day",
            amount: 40,
        }},
    }},
    {$lookup:{
        from:"country_vac_manu",
        localField:"twentydayslater",
        foreignField:"date",
        pipeline:[
            {$match:{country:"Germany"}},
            {$project:{_id:0,date:1,vaccine:1,total_vaccinations:1}}
            ],
            as: "twentyVac"
    }},
    {$lookup:{
        from:"country_vac_manu",
        localField:"thirtydayslater",
        foreignField:"date",
        pipeline:[
            {$match:{country:"Germany"}},
            {$project:{_id:0,date:1,vaccine:1,total_vaccinations:1}}
            ],
            as: "thirtyVac"
    }},
    {$lookup:{
        from:"country_vac_manu",
        localField:"fortydayslater",
        foreignField:"date",
        pipeline:[
            {$match:{country:"Germany"}},
            {$project:{_id:0,date:1,vaccine:1,total_vaccinations:1}}
            ],
            as: "fortyVac"
    }},
    //{$match:{twentyVac:{$ne:[]}}}, /* comment out to remove the null values before 12-07-2020 */
    {$project:{_id:0,country:1,date:1,new_cases:1,"TwentyDaysLaterVacData":"$twentyVac",
    "ThirtyDaysLaterVacData":"$thirtyVac","FortyDaysLaterVacData":"$fortyVac"}}
])

// Question 20: Vaccination Effects. Specific to Germany, on a daily basis, based on the total number of accumulated vaccinations (sum of total_vaccinations of each vaccine in a day), generate the daily new cases after 21 days, 60 days, and 120 days.
db.country_vac_manu.aggregate([
    {$match:{country:"Germany"}},
    {$group:{_id:{groupbydate:"$date",groupbycountry:"$country"},"vacdata":{$push:{"Vaccine":"$vaccine","VaccinationsAdministered":"$total_vaccinations"}}}},
    {$project:{
        _id:0,
        "date":"$_id.groupbydate",
        "country":"$_id.groupbycountry",
        "total_vac": {$sum: "$vacdata.VaccinationsAdministered"},
        "threeweekslater":{$dateAdd: { 
            startDate:"$_id.groupbydate",
            unit: "day",
            amount: 21,
        }},
        "twomonthslater":{$dateAdd: { 
            startDate:"$_id.groupbydate" ,
            unit: "day",
            amount: 60,
        }},
        "fourmonthslater":{$dateAdd: { 
            startDate:"$_id.groupbydate" ,
            unit: "day",
            amount: 120,
        }}
    }},
    {$lookup:{
        from:"covid19data2",
        localField:"threeweekslater",
        foreignField:"date",
        pipeline:[
            {$match:{country:"Germany"}},
            {$project:{_id:0,date:1,new_cases:1}}
            ],
            as: "threeweeks"
    }},
    {$lookup:{
        from:"covid19data2",
        localField:"twomonthslater",
        foreignField:"date",
        pipeline:[
            {$match:{country:"Germany"}},
            {$project:{_id:0,date:1,new_cases:1}}
            ],
            as: "twomonths"
    }},
    {$lookup:{
        from:"covid19data2",
        localField:"fourmonthslater",
        foreignField:"date",
        pipeline:[
            {$match:{country:"Germany"}},
            {$project:{_id:0,date:1,new_cases:1}}
            ],
            as: "fourmonths"
    }},
    {$project: {"Date":"$date","Country":"$country","Total_number_of_accumulated_vaccinations":"$total_vac","CasesIn21Days":"$threeweeks","CasesIn60Days":"$twomonths","CasesIn120Days":"$fourmonths"}},
    {$sort:{Date:1}}
])


