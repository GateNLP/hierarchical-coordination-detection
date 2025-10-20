import dayjs from "dayjs";

export function calcPostDistribution(posts, data) {
    // this is the data structure for the heatmap, just set up for the days to start with
    var heatmap = [
        {
            z: new Array(7),
            y: ['Sunday','Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday','Saturday'],
            type: 'heatmap',
            hoverongaps: false,
            colorscale: 'Portland',
            hovertemplate: "%{z}<extra>%{y} at %{x:02d}:xx</extra>"
        }            
    ];

    // then we add empty values for the hours of each day
    for (var d = 0 ; d < 7 ; ++d)
        heatmap[0].z[d] = new Array(24).fill(0);

    // then we setup the data structure for the scatter plot time series, but again
    // without any data in it
    var timeseries = [
        {
            type: 'bar',
            mode: 'lines+markers',
            x: [],
            y: [],
            marker: {
                size: 10
            }
        }
    ]

    // this object will allow us to store the number of posts per day
    // which we need to build the time series
    var days = {};

    // for each post selected by the filter
    posts.forEach((p) => {
        
        // get the date from the raw data
        // this is horrible but takes account of the fact the CSV data
        // is still a string while the ES data is a unix timestamp
        var date = Number.isInteger(data.posts[p].time) ? dayjs.unix(data.posts[p].time) : dayjs(data.posts[p].time);

        // add 1 to the heatmap for the relevant day/hour
        heatmap[0].z[date.day()][date.hour()]++

        var day = date.format("YYYY-MM-DD");
        if (!days[day]) {
            // find out if we've already seen a post for this
            // day or not, and if not then set the count to 1
            days[day] = 1
        } else {
            // otherwise just add one to the existing count
            ++days[day];
        }
    })

    // this just sets any zero elements in the heatmap to null
    // this means we end up with an empty square which is easier
    // to see than a zero value
    for (var d = 0 ; d < 7 ; ++d)
        for (var h = 0 ; h < 24 ; ++h)
            if (heatmap[0].z[d][h] === 0) heatmap[0].z[d][h] = null

    // this converts the object holding the day/count values into two
    // arrays that are in the right format for Plotly.js to render the
    // scatter plot 
    Object.keys(days).sort().forEach((d) => {
        timeseries[0].x.push(d);
        timeseries[0].y.push(days[d])
    })

    return [timeseries, heatmap]
}