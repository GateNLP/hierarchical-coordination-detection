import { useState } from "react";

import WordCloud from "react-d3-cloud";

import { scaleSqrt } from "d3-scale";

import React from "react";

import Tabs from '@mui/joy/Tabs';
import TabList from '@mui/joy/TabList';
import Tab from '@mui/joy/Tab';
import TabPanel from '@mui/joy/TabPanel';
import TableChartIcon from '@mui/icons-material/TableChart';
import BubbleChartIcon from '@mui/icons-material/BubbleChart';
import CommentIcon from '@mui/icons-material/Comment';

import Table from '@mui/material/Table';
import TableBody from '@mui/material/TableBody';
import TableCell from '@mui/material/TableCell';
import TableContainer from '@mui/material/TableContainer';
import TableHead from '@mui/material/TableHead';
import TableRow from '@mui/material/TableRow';
import Paper from '@mui/material/Paper';

import Typography from '@mui/material/Typography';

import InputLabel from '@mui/material/InputLabel';
import MenuItem from '@mui/material/MenuItem';
import FormControl from '@mui/material/FormControl';
import Select from '@mui/material/Select';
import Box from '@mui/material/Box';

import { PostDisplay } from "./PostDisplay";

import { TimeGap } from "./TimeGap";

import InfiniteScroll from "react-infinite-scroll-component";


const randSeed = Math.random() * 4294967296;

export const EdgeDisplay = (props) => {

    const { anonymize, edge, graph, posts, addToFilter, jobID } = props;

    const [hashtag, setHashtag] = useState('');

    const [loaded, setLoaded] = React.useState(0);

     const fetch = () => {
        setLoaded(Math.min(loaded+10,timelines[hashtag].length))
    }

    if (edge === null || graph === null) return null;

    const handleChange = (event) => {
        var h = parseInt(event.target.value)
        setHashtag(h);
        setLoaded(Math.min(10,timelines[h].length));
    };

    const onHashtagClick = (event, word) => {
        addToFilter("hashtag", word);
    };

    const attributes = graph.getEdgeAttributes(edge);

    const source = graph.getNodeAttributes(graph.source(edge));
    const target = graph.getNodeAttributes(graph.target(edge));

    const hashtags = [];
    const timelines = [];


    for (var i = 0; i < attributes.hashtags.length; ++i) {
        hashtags.push({ text: attributes.hashtags[i], value: attributes.weights[i] })

        timelines.push(attributes.source[i].concat(attributes.target[i]).sort());
    }

    const scale = scaleSqrt().domain([attributes.weights[attributes.weights.length - 1], attributes.weights[0]]).range([11.25, 45]);

    let randState = randSeed;
    const randFunction = () => {
        let t = randState += 0x6D2B79F5;
        t = Math.imul(t ^ (t >>> 15), t | 1);
        t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
        return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };

    return (

        <div>

            <Typography variant={"h5"}>{anonymize(source.label)} and {anonymize(target.label)} have a coordination weight of {attributes.weight.toFixed(4)}</Typography>

            <Tabs orientation="vertical" defaultValue={"overview"}>


                <TabPanel value={"overview"}>
                    <Typography variant={"body2"}>{anonymize(source.label)} and {anonymize(target.label)} shared the following links of interest. The size
                    denotes the strength of the coordination for each link.</Typography>

                    <WordCloud data={hashtags}
                        font="Arial"
                        fontWeight="bold"
                        rotate={() => 0}
                        fontSize={(word) => scale(word.value)}
                        height={300}
                        random={randFunction}
                        onWordClick={onHashtagClick}
                    />

                </TabPanel>

                <TabPanel value="data">
                    <TableContainer component={Paper} sx={{ maxHeight: 300 }}>

                        <Table stickyHeader size='small'>
                            <TableHead>
                                <TableRow>
                                    <TableCell>Link</TableCell>
                                    <TableCell>Weight</TableCell>
                                </TableRow>
                            </TableHead>
                            <TableBody>
                                {
                                    hashtags.map((row,id) => {
                                        return (<TableRow key={"hashtag_"+id}>
                                            <TableCell>{row.text}</TableCell>
                                            <TableCell>{row.value.toFixed(4)}</TableCell>
                                        </TableRow>)
                                    })
                                }
                            </TableBody>
                        </Table>
                    </TableContainer>
                </TabPanel>

                <TabPanel value={"posts"}>
                    <FormControl fullWidth>
                        <InputLabel id="hashtag-select-label">Link</InputLabel>
                        <Select
                            labelId="hashtag-select-label"
                            id="hashtag-select"
                            value={hashtag}
                            label="Link"
                            onChange={handleChange}
                        >
                            {
                                hashtags.map((row, i) => {
                                    return (<MenuItem key={"menu_"+i} value={i}>{row.text}</MenuItem>)
                                })
                            }
                        </Select>
                    </FormControl>

                    {hashtag !== "" && hashtag < timelines.length &&  <Paper sx={{mt: 2, pt:1 }}>
                        <Typography sx={{m:1}} variant="body2"><span style={{}}>{anonymize(source.label)}</span><span style={{float:"right"}}>{anonymize(target.label)}</span></Typography>
                        <div style={{maxHeight: 450, overflow: "auto"}} iud="scrollable">

                            <InfiniteScroll
                                dataLength={loaded}
                                next={fetch}
                                hasMore={loaded < timelines[hashtag].length}
                                scrollableTarget="scrollable"
                                                        >
                                {
                                    timelines[hashtag].slice(0,loaded).map((post, index) => {
                                        var sx = {};

                                        if (attributes.source[hashtag].includes(post))
                                            sx = {mr: 5, backgroundColor: "#dbf4fd"}
                                        else
                                            sx = {ml: 5, backgroundColor: "#f2f6f9"}

                                        var show = index !== 0 && (attributes.source[hashtag].includes(post) !== attributes.source[hashtag].includes(timelines[hashtag][index-1]));

                                        return (
                                            <React.Fragment key={"pos_"+index}>
                                                {show && <TimeGap posts={posts} post1={timelines[hashtag][index-1]} post2={post}/>}
                                                <PostDisplay anonymize={anonymize} sx={sx} post={posts && posts[post].text ? posts[post] : post} jobID={jobID} />
                                            </React.Fragment>
                                        )
                                    })
                                }
                            </InfiniteScroll>
                        </div>
                    </Paper>
                    }

                </TabPanel>

                <TabList style={{ boxShadow: "none" }}>

                    <Tab style={{ float: "right" }} value={"overview"}><BubbleChartIcon color="action" /></Tab>
                    <Tab style={{ float: "right" }} value={"data"}><TableChartIcon color="action" /></Tab>
                    <Tab style={{ float: "right" }} value={"posts"}><CommentIcon color="action" /></Tab>
                </TabList>

            </Tabs>
        </div >
    )
}
