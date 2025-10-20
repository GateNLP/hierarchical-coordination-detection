import React, { memo } from "react";

import Plotly from 'plotly.js-dist-min'
import createPlotlyComponent from 'react-plotly.js/factory';

import Typography from '@mui/material/Typography';

import dayjs from "dayjs";

const Plot = createPlotlyComponent(Plotly);

export const AccountAge = memo(function AccountAge(props) {
    const { anonymize, graph, filter, range, data } = props;

    if (graph === null) return null;

    var startDate = dayjs.unix(data.range[0]).format("YYYY-MM-DD")

    var events = [{
        y0: 0,
        y1: 1,
        yref: "paper",
        type: "line",
        x0: startDate,
        x1: startDate,
        line: {
            color: 'grey',
            width: 1.5,
            dash: 'dot'
        },
        label: {
            text: "Dataset Starts",
            font: { size: 12, color: "rgb(68, 68, 68)" },
            textposition: 'end',
            xanchor: 'left',
            textangle: 90
        }
    }]

    var posts = {
        x: [],
        y: [],
        text: [],
        mode: 'markers',
        marker: {
            color: [],
            size: [],
        }
    };

    var followers = {
        x: [],
        y: [],
        text: [],
        mode: 'markers',
        marker: {
            color: [],
            size: [],
        }
    };

    var nodes = new Set();

    var minDate = 0;
    var maxDate = 0;

    // now go through all the edges, find those which are currently
    // selected by the filters and then add them to the set of posts
    graph.forEachEdge((edge, edgeAttrs, source, target, sourceAttrs, targetAttrs) => {

        if (minDate === 0) {
            minDate = Math.min(sourceAttrs["created_at"], targetAttrs["created_at"]);
            maxDate = Math.max(sourceAttrs["created_at"], targetAttrs["created_at"]);
        } else {
            minDate = Math.min(minDate,Math.min(sourceAttrs["created_at"], targetAttrs["created_at"]));
            maxDate = Math.max(maxDate,Math.max(sourceAttrs["created_at"], targetAttrs["created_at"]));
        }

        // NOTE: this is the same code as in RawEdgeDisplay. It might make
        //       sense to move this to a util function so both can use the
        //       same code; easier to maintain in the long run.

        var weight = edgeAttrs.weight;

        if (weight < range[0] || weight > range[1]) return null;

        var display = filter === null || filter.users.length === 0 ||
            filter.users.includes(anonymize(sourceAttrs.label).toLowerCase()) ||
            filter.users.includes(anonymize(targetAttrs.label).toLowerCase());

        if (display && filter !== null && filter.hashtags.length > 0) {
            var matches = 0;

            edgeAttrs.hashtags.forEach(hashtag => {
                if (filter.hashtags.includes(hashtag.toLowerCase()))
                    ++matches;
            });

            display = (filter.hashtagMode === "any" ? matches !== 0 : matches === filter.hashtags.length);

        }

        if (display) {
           nodes.add(source)
           nodes.add(target);
        }
    })

    //graph.forEachNode((node, attributes) => {
    nodes.forEach((node) => {

        var attributes = graph.getNodeAttributes(node)

        var date = dayjs.unix(attributes["created_at"])

        posts["x"].push(date.format("YYYY-MM-DD"))
        posts["y"].push(attributes["posts"].length)
        posts["text"].push(anonymize(attributes["label"]));
        posts["marker"]["color"].push(attributes["color"])
        posts["marker"]["size"].push(attributes["size"]*3)

        followers["x"].push(date.format("YYYY-MM-DD"))
        followers["y"].push(attributes["followers"])
        followers["text"].push(anonymize(attributes["label"]));
        followers["marker"]["color"].push(attributes["color"])
        followers["marker"]["size"].push(attributes["size"]*3)
    })

    minDate = dayjs.unix(minDate).format("YYYY-MM-DD");
    maxDate = dayjs.unix(maxDate).format("YYYY-MM-DD");

    return (
         <React.Fragment>
            <Typography paragraph variant={"body1"}>This graph allows you to see the relationship between account age and the activity of the accounts. Each bubble represents a single account and is sized in the same way as on the main network visualization, namely the number of edges it is part of. The colour of each node is also the same and represents the community it belongs to.</Typography>
            <Plot style={{ width: "100%", height:"40vw" }} data={[posts]} layout={{shapes: events, margin: { t: 10, b: 300, l: 100, r: 50 }, font:{size:14, family: '"Roboto", "Helvetica", "Arial", sans-serif'}, xaxis:{type:"date", title: "Account Creation Date", range: [minDate, maxDate]}, yaxis: { title: "Number of Posts", rangemode:"tozero", autorange:true}, autosize: true }} config={{ responsive: true, 'displayModeBar': false }}  />

            <Plot style={{ width: "100%", height:"40vw" }} data={[followers]} layout={{shapes: events, margin: { t: 10, b: 300, l: 100, r: 50 }, font:{size:14, family: '"Roboto", "Helvetica", "Arial", sans-serif'}, xaxis:{type:"date", title: "Account Creation Date", range: [minDate, maxDate]}, yaxis: { title: "Number of Followers", rangemode:"tozero", autorange:true}, autosize: true }} config={{ responsive: true, 'displayModeBar': false }}  />
        </React.Fragment>
    )
})