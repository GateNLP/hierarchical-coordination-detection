import React, { memo } from "react";

import Plotly from 'plotly.js-dist-min'
import createPlotlyComponent from 'react-plotly.js/factory';

import Typography from '@mui/material/Typography';

import { calcPostDistribution } from "./api";

const Plot = createPlotlyComponent(Plotly);

export const PostTimes = memo(function PostTimes(props) {
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

    const [timeseries, heatmap] = calcPostDistribution(posts, data)

    return (
        <React.Fragment>
            <Typography paragraph variant={"body"}>This graph shows the dates on which the posts relating to the currently filtetred edges fall:</Typography>
            <Plot style={{ width: "100%", height:"30vw" }} data={timeseries} layout={{ margin: { t: 10, b: 30, l: 100, r: 50 }, font:{size:14, family: '"Roboto", "Helvetica", "Arial", sans-serif'}, autosize: true, xaxis: {type: "date"}, yaxis: { title: "Number of Posts", rangemode:"tozero", autorange:true} }} config={{ responsive: true, 'displayModeBar': false }}  />
            
            <Typography paragraph variant={"body"}>This heatmap shows the hour of the day at which the posts relating to the currently filtered edges were sent:</Typography>
            <Plot style={{ width: "100%", height:"30vw" }} data={heatmap} layout={{ margin: { t: 10, b: 60, l: 110, r: 50 }, font:{size:14, family: '"Roboto", "Helvetica", "Arial", sans-serif'}, autosize: true, xaxis: {title: "Hour of the Day", dtick: 1}, yaxis: {title: "Day of the Week"} }} config={{ responsive: true, 'displayModeBar': false }}  />
        </React.Fragment>
    )
})