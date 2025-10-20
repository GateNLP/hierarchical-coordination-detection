import React from "react";

import WordCloud from "react-d3-cloud";

import { scaleSqrt } from "d3-scale";

import Tabs from '@mui/joy/Tabs';
import TabList from '@mui/joy/TabList';
import Tab from '@mui/joy/Tab';
import TabPanel from '@mui/joy/TabPanel';
import TableChartIcon from '@mui/icons-material/TableChart';
import BubbleChartIcon from '@mui/icons-material/BubbleChart';
import Box from '@mui/material/Box';

import Table from '@mui/material/Table';
import TableBody from '@mui/material/TableBody';
import TableCell from '@mui/material/TableCell';
import TableContainer from '@mui/material/TableContainer';
import TableHead from '@mui/material/TableHead';
import TableRow from '@mui/material/TableRow';
import Paper from '@mui/material/Paper';

import Typography from '@mui/material/Typography';

import CommentIcon from '@mui/icons-material/Comment';
import PeopleIcon from '@mui/icons-material/People';
import ScheduleIcon from '@mui/icons-material/Schedule';

import { PostDisplay } from "./PostDisplay";
import { Circle } from "@mui/icons-material";

import Plotly from 'plotly.js-dist-min'
import createPlotlyComponent from 'react-plotly.js/factory';

import { calcPostDistribution } from "./api";
import InfiniteScroll from "react-infinite-scroll-component";

const Plot = createPlotlyComponent(Plotly);

const randSeed = Math.random() * 4294967296;

export const NodeDisplay = (props) => {
    const { anonymize, node, graph, addToFilter, jobID, data } = props;

    const onUserClick = (event, word) => {
        //console.log(`onWordClick: ${word}`);
        addToFilter("user", word);
    };

    const onHashtagClick = (event, word) => {
        //console.log(`onWordClick: ${word}`);
        addToFilter("hashtag", word);
    };

    if (node === null || graph === null) return null;

    const attributes = graph.getNodeAttributes(node)

    const community = attributes.community;
    const color = attributes.color;

    const communityHashtagsData = data.communities.links[community];
    const cucData = data.communities.nodes[community];

    const edges = graph.edges(node);

    var cloudData = {};

    const userData = {}

    const [loaded, setLoaded] = React.useState(Math.min(10,attributes.posts.length));

    const users = [];
    edges.forEach(edge => {


        const source = graph.source(edge);
        const target = graph.target(edge);

        const other = source === node ? target : source;

        // the size has already been sqrt scaled and then we do it again
        // when building the word cloud, do we need to?
        userData[ graph.getNodeAttributes(other).label] = graph.getEdgeAttributes(edge).size;
        users.push({ text: anonymize(graph.getNodeAttributes(other).label), value: graph.getEdgeAttributes(edge).size, weight: graph.getEdgeAttributes(edge).weight })

        const hashtags = graph.getEdgeAttributes(edge).hashtags;

        hashtags.forEach(hashtag => {
            cloudData[hashtag] = 1 + (hashtag in cloudData ? cloudData[hashtag] : 0);
        });
    });

    const cloud = [];
    Object.keys(cloudData).forEach(hashtag => {
        cloud.push({ text: hashtag, value: cloudData[hashtag] })
    });

    const communityHashtagCloud = [];
    Object.keys(communityHashtagsData).forEach(hashtag => {
        communityHashtagCloud.push({text: hashtag, value: communityHashtagsData[hashtag]})
    });

    var communityUserCloud = [];
    Object.keys(cucData).forEach(node => {
        communityUserCloud.push({text: anonymize(node), value: cucData[node]})
    })

    cloud.sort((a, b) => b.value - a.value);

    users.sort((a, b) => b.value - a.value);

    const scale = scaleSqrt().domain([Math.min(...Object.values(cloudData)), Math.max(...Object.values(cloudData))]).range([11.25, 45]);

    const userScale = scaleSqrt().domain([Math.min(...Object.values(userData)), Math.max(...Object.values(userData))]).range([11.25, 45]);

    const communityUserScale = scaleSqrt().domain([Math.min(...Object.values(cucData)), Math.max(...Object.values(cucData))]).range([11.25, 45]);

    const communityHashtagScale = scaleSqrt().domain([Math.min(...Object.values(communityHashtagsData)), Math.max(...Object.values(communityHashtagsData))]).range([11.25, 45]);

    const bioScale = data.communities.hasOwnProperty("bios") && data.communities["bios"][community].length > 0 ? scaleSqrt().domain([data.communities["bios"][community].slice(-1)[0]["value"], data.communities["bios"][community][0]["value"]]).range([11.25, 45]) : null;
    
    const textScale = data.communities.hasOwnProperty("text") && data.communities["text"][community].length > 0 ? scaleSqrt().domain([data.communities["text"][community].slice(-1)[0]["value"], data.communities["text"][community][0]["value"]]).range([11.25, 45]) : null;

    let randState = randSeed;
    const randFunction = () => {
        let t = randState += 0x6D2B79F5;
        t = Math.imul(t ^ (t >>> 15), t | 1);
        t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
        return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };

    
    const fetch = () => {       
        setLoaded(Math.min(loaded+10,attributes.posts.length))
    }

    const [timeseries, heatmap] = calcPostDistribution(attributes.posts, data)

    return (
        <div>
            <Typography variant={"h5"}>{anonymize(attributes.label)}</Typography>

            <Tabs orientation="vertical" defaultValue={"overview"}>


                <TabPanel value={"overview"}>
                    <Typography variant={"body2"}>{anonymize(attributes.label)} shared the same links of interest as the following users. The usernames are scaled
                    so that the larger the name the stronger the detected coordination.</Typography>
                    <WordCloud data={users}
                        font="Arial"
                        fontWeight="bold"
                        rotate={() => 0}
                        fontSize={(word) => userScale(word.value)}
                        height={300}
                        random={randFunction}
                        onWordClick={onUserClick}
                    />

                    <Typography variant={"body2"}>{anonymize(attributes.label)} shared the following links of interest. Size denotes the number of other users
                    who shared the same links.</Typography>
                    <WordCloud data={cloud}
                        font="Arial"
                        fontWeight="bold"
                        rotate={() => 0}
                        fontSize={(word) => scale(word.value)}
                        height={300}
                        random={randFunction}
                        onWordClick={onHashtagClick}
                    /></TabPanel>



                <TabPanel value="data">
                    <TableContainer component={Paper} sx={{ maxHeight: 300 }}>

                        <Table stickyHeader size='small'>
                            <TableHead>
                                <TableRow>
                                    <TableCell>User</TableCell>
                                    <TableCell>Weight</TableCell>
                                </TableRow>
                            </TableHead>
                            <TableBody>
                                {
                                    users.map((row, id) => {
                                        return (<TableRow key={"user_"+id}>
                                            <TableCell>{row.text}</TableCell>
                                            <TableCell>{row.weight.toFixed(4)}</TableCell>
                                        </TableRow>)
                                    })
                                }
                            </TableBody>
                        </Table>
                    </TableContainer>

                    <Box mt={5} />

                    <TableContainer component={Paper} sx={{ maxHeight: 300 }}>

                        <Table stickyHeader size='small'>
                            <TableHead>
                                <TableRow>
                                    <TableCell>Link</TableCell>
                                    <TableCell>Edges</TableCell>
                                </TableRow>
                            </TableHead>
                            <TableBody>
                                {
                                    cloud.map((row,id) => {
                                        return (<TableRow key={"hashtag_"+id}>
                                            <TableCell>{row.text}</TableCell>
                                            <TableCell>{row.value}</TableCell>
                                        </TableRow>)
                                    })
                                }
                            </TableBody>
                        </Table>
                    </TableContainer>

                </TabPanel>

                <TabPanel value={"posts"}>
                    <Typography variant={"body2"}>{anonymize(attributes.label)} made the following {attributes.posts.length} posts</Typography>

                    <Paper sx={{ maxHeight: 450 , overflow:"auto"}} id="scrollable">
                        
                        <InfiniteScroll
                            dataLength={loaded}
                            next={fetch}
                            hasMore={loaded < attributes.posts.length}
                            scrollableTarget="scrollable"
                            >
                            {attributes.posts.sort().slice(0,loaded).map((post,id) => {
                                return (<PostDisplay anonymize={anonymize} key={"post_"+id} post={data.posts && data.posts[post].text ? data.posts[post] : post} jobID={jobID} />)
                            })}
                        </InfiniteScroll>
                        
                    </Paper>

                </TabPanel>

                <TabPanel value={"times"}>
                    <Typography variant={"body2"}>{anonymize(attributes.label)} made {attributes.posts.length} posts at the following times</Typography>

                    <Plot style={{ width: "100%", maxHeight:"250px" }} data={timeseries} layout={{ margin: { t: 10, b: 30, l: 100, r: 50 }, font:{size:14, family: '"Roboto", "Helvetica", "Arial", sans-serif'}, autosize: true, xaxis: {type: "date"}, yaxis: { title: "Number of Posts", rangemode:"tozero", autorange:true} }} config={{ responsive: true, 'displayModeBar': false }}  />

                    <Plot style={{ width: "100%", height:"250px" }} data={heatmap} layout={{ margin: { t: 10, b: 60, l: 110, r: 50 }, font:{size:14, family: '"Roboto", "Helvetica", "Arial", sans-serif'}, autosize: true, xaxis: {title: "Hour of the Day", dtick: 1}, yaxis: {title: "Day of the Week"} }} config={{ responsive: true, 'displayModeBar': false }}  />
                    
                </TabPanel>

                <TabPanel value={"community"} style={{overflow: "auto", maxHeight:450}}>
                    <Typography variant={"body2"}><Circle style={{color: color, verticalAlign: "middle"}}/> {anonymize(attributes.label)} is in a cluster with {communityUserCloud.length-1} other nodes.</Typography>

                    <WordCloud data={communityUserCloud}
                        font="Arial"
                        fontWeight="bold"
                        rotate={() => 0}
                        fontSize={(word) => communityUserScale(word.value)}
                        height={300}
                        random={randFunction}
                        onWordClick={onUserClick}
                    />

                    <Typography variant={"body2"}>Edges within the community involve the sharing of the following links:</Typography>
                    <WordCloud data={communityHashtagCloud}
                        font="Arial"
                        fontWeight="bold"
                        rotate={() => 0}
                        fontSize={(word) => communityHashtagScale(word.value)}
                        height={300}
                        random={randFunction}
                        onWordClick={onHashtagClick}
                    />

                    {bioScale != null && <React.Fragment><Typography variant={"body2"}>These are the significant terms from the user descriptions in this community:</Typography>
                    <WordCloud data={data.communities["bios"][community]}
                        font="Arial"
                        fontWeight="bold"
                        rotate={() => 0}
                        fontSize={(word) => bioScale(word.value)}
                        height={300}
                        random={randFunction}
                    /></React.Fragment>}

                    {textScale != null && <React.Fragment><Typography variant={"body2"}>These are the significant terms from the posts in this community:</Typography>
                    <WordCloud data={data.communities["text"][community]}
                        font="Arial"
                        fontWeight="bold"
                        rotate={() => 0}
                        fontSize={(word) => textScale(word.value)}
                        height={300}
                        random={randFunction}
                    /></React.Fragment>}
                </TabPanel>

                <TabList style={{ boxShadow: "none" }}>

                    <Tab style={{ float: "right" }} value={"overview"}><BubbleChartIcon color="action" /></Tab>
                    <Tab style={{ float: "right" }} value={"data"}><TableChartIcon color="action" /></Tab>
                    <Tab style={{ float: "right" }} value={"times"}><ScheduleIcon color="action" /></Tab>
                    <Tab style={{ float: "right" }} value={"posts"}><CommentIcon color="action" /></Tab>
                    <Tab style={{ float: "right" }} value={"community"}><PeopleIcon color="action" /></Tab>
                </TabList>
            </Tabs>
        </div>);
}