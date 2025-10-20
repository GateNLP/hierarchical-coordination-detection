import React, { memo } from "react";

import Plotly from 'plotly.js-dist-min'
import createPlotlyComponent from 'react-plotly.js/factory';

import Typography from '@mui/material/Typography';

import dayjs from "dayjs";

const Plot = createPlotlyComponent(Plotly);

export const LinkTimes = memo(function LinkTimes(props) {
    const { anonymize, graph, filter, range, data } = props;

    if (graph === null) return null;


    // this stores the set of unique posts that we want to count for the two viz
    var posts = new Set();

    // now go through all the edges, find those which are currently
    // selected by the filters and then add them to the set of posts
    graph.forEachEdge((edge, edgeAttrs, source, target, sourceAttrs, targetAttrs) => {

        // NOTE: this is the same code as in RawEdgeDisplay. It might make
        //       sense to move this to a util function so both can use the
        //       same code; easier to maintain in the long run.

        var weight = edgeAttrs.weight;

        if (weight < range[0] || weight > range[1]) return null;

        var source = sourceAttrs.label;
        var target = targetAttrs.label;

        var display = filter === null || filter.users.length === 0 ||
            filter.users.includes(anonymize(source).toLowerCase()) ||
            filter.users.includes(anonymize(target).toLowerCase());

        if (display && filter !== null && filter.hashtags.length > 0) {
            var matches = 0;

            edgeAttrs.hashtags.forEach(hashtag => {
                if (filter.hashtags.includes(hashtag.toLowerCase()))
                    ++matches;
            });

            display = (filter.hashtagMode === "any" ? matches !== 0 : matches === filter.hashtags.length);

        }

        if (display) {
            // this is the bit that differs from RawEdgeDisplay as it
            // adds all the posts form the edges to the set

            edgeAttrs.source.forEach((a) => {
                a.forEach((b) => {
                    posts.add(b)
                })
            })

            edgeAttrs.target.forEach((a) => {
                a.forEach((b) => {
                    posts.add(b)
                })
            })
        }
    })

    var links = {}

    // for each post selected by the filter
    posts.forEach((p) => {
        data.posts[p].links.forEach((l) => {
            if (!links[l]) {
                links[l] = 1
            } else {
                ++links[l]
            }
        })
    })
    
    var top10 = [];

    // then we setup the data structure for the scatter plot time series, but again
    // without any data in it
    var timeseries = [
        
    ]
    
    Object.entries(links).sort((a, b) => b[1] - a[1]).slice(0,5).forEach((a) => {
        var link = a[0]

        // this object will allow us to store the number of posts per day
        // which we need to build the time series
        var days = {};


        posts.forEach((p) => {
            if (data.posts[p].links.indexOf(link) !== -1) {
                // get the date from the raw data
                var date = dayjs.unix(data.posts[p].time)
                
                var day = date.format("YYYY-MM-DD");
                if (!days[day]) {
                    // find out if we've already seen a post for this
                    // day or not, and if not then set the count to 1
                    days[day] = 1
                } else {
                    // otherwise just add one to the existing count
                    ++days[day];
                }
            }
                
        })

        var trace = {
            type: 'scatter',
            mode: 'lines+markers',
            name: a[0],
            x: [],
            y: [],
            marker: {
                size: 10
            }
        }

        Object.keys(days).sort().forEach((d) => {
            trace.x.push(d);
            trace.y.push(days[d])
        })

        timeseries.push(trace)
    })

    return (
        <React.Fragment>
            <Typography paragraph variant={"body"}>This graph shows the top 5 links (from the filtetred edges) as they occur over time:</Typography>
            <Plot style={{ width: "100%", height:"40vw" }} data={timeseries} layout={{ margin: { t: 10, b: 300, l: 100, r: 50 }, font:{size:14, family: '"Roboto", "Helvetica", "Arial", sans-serif'}, autosize: true, yaxis: { title: "Number of Posts", rangemode:"tozero", autorange:true} }} config={{ responsive: true, 'displayModeBar': false }}  />
        </React.Fragment>
    )
})