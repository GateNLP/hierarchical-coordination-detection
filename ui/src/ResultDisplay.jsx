import React, { useEffect, useState } from "react";
import { SigmaContainer, useLoadGraph, useRegisterEvents, useSetSettings, useSigma, ZoomControl, ControlsContainer } from "@react-sigma/core";

import Tabs from '@mui/joy/Tabs';
import TabList from '@mui/joy/TabList';
import Tab from '@mui/joy/Tab';
import TabPanel from '@mui/joy/TabPanel';

import Typography from '@mui/material/Typography';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import Paper from '@mui/material/Paper';

import TextField from '@mui/material/TextField';
import Slider from '@mui/material/Slider';

import Select from '@mui/material/Select';
import MenuItem from '@mui/material/MenuItem';

import TableChartIcon from '@mui/icons-material/TableChart';
import BubbleChartIcon from '@mui/icons-material/BubbleChart';
import ScheduleIcon from '@mui/icons-material/Schedule';
import LinkIcon from '@mui/icons-material/Link';
import PersonIcon from '@mui/icons-material/Person';

import "@react-sigma/core/lib/react-sigma.min.css";

import { NodeDisplay } from "./NodeDisplay";
import { EdgeDisplay } from "./EdgeDisplay";

import Grid from '@mui/material/Grid';

import { RawEdgeDisplay } from './RawEdgeDisplay'
import { Link } from "@mui/material";
import { PostTimes } from "./PostTimes";
import { LinkTimes } from "./LinkTimes";
import { AccountAge } from "./AccountAge";
import {getUniqueColor} from "./App.jsx";

const LoadGraph = (props) => {

  const { anonymize, graph, clickedNode, setClickedNode, clickedEdge, setClickedEdge, range, filter, displayType } = props;

  const loadGraph = useLoadGraph();
  const registerEvents = useRegisterEvents();

  const setSettings = useSetSettings();
  const [hoveredNode, setHoveredNode] = useState(null);
  const [draggedNode, setDraggedNode] = useState(null);

  const sigma = useSigma()

  useEffect(() => {

    registerEvents({
      clickNode: ({node}) => {
        if (sigma.getGraph().getNodeAttribute(node, "dragging")) {
          // don't treat the release at the end of a drag as a click event
          sigma.getGraph().removeNodeAttribute(node, "dragging");
          return;
        }
        setClickedNode((clickedNode) => {
          if (clickedNode === node)
            return null;
          else {
            setClickedEdge(null)
            return node;
          }
        });
      },
      enterNode: (event) => setHoveredNode(event.node),
      leaveNode: () => setHoveredNode(null),

      clickEdge: ({edge}) => {
        setClickedEdge((clickedEdge) => {
          if (clickedEdge === edge)
            return null;
          else {
            setClickedNode(null);
            return edge;
          }
        });
      },
      downNode: (e) => {
        if (!e.event.original.shiftKey) return false;
        const node = e.node;
        setDraggedNode(node);
        sigma.getGraph().setNodeAttribute(node, "highlighted", true);
        sigma.getGraph().setNodeAttribute(node, "dragging", true);

        // add a mouse move handler to change the position of the dragged node
        /**
         * @param e {import("sigma/types").MouseCoords}
         */
        function moveHandler(e) {
          // Get new position of node
          const pos = sigma.viewportToGraph(e);
          sigma.getGraph().setNodeAttribute(node, "x", pos.x);
          sigma.getGraph().setNodeAttribute(node, "y", pos.y);

          // Prevent sigma to move camera:
          e.preventSigmaDefault();
          e.original.preventDefault();
          e.original.stopPropagation();
        }

        // when mouse is released, clear the dragged node and remove the dragging handlers
        function upHandler() {
          setDraggedNode(null);
          sigma.getGraph().removeNodeAttribute(node, "highlighted");
          sigma.getMouseCaptor().off("mousemovebody", moveHandler);
          sigma.getMouseCaptor().off("mouseup", upHandler);
        }

        sigma.getMouseCaptor().on("mousemovebody", moveHandler);
        sigma.getMouseCaptor().on("mouseup", upHandler);
      },
      // Disable the autoscale at the first down interaction
      mousedown: () => {
        if (!sigma.getCustomBBox()) sigma.setCustomBBox(sigma.getBBox());
      }
    });
  }, [registerEvents, sigma, setClickedNode, setClickedEdge]);

  useEffect(() => {
    loadGraph(graph);
  }, [loadGraph, graph]);

  useEffect(() => {
    setSettings({
      labelSize: 20,
      nodeReducer: (node, data) => {
        const sigmaGraph = sigma.getGraph();
        let hidden = true;
        const newData = { ...data, highlighted: data.highlighted || false, label: "", hidden };

        sigmaGraph.edges(node).forEach(edge => {
          var weight = sigmaGraph.getEdgeAttributes(edge).weight;

          // show the node if at least one edge leading from it will be
          // visible given the current weight restriction
          hidden = hidden && (weight < range[0] || weight > range[1])
        });

        if (hoveredNode === node) hidden = false;

        if (clickedNode === node) {
          hidden = false;
          newData.highlighted = true;
          newData.label = anonymize(data.label);
        }

        if (filter !== null && filter.users.length > 0) {

          if (filter.users.includes(anonymize(data.label).toLowerCase())) {
            hidden = false;

            // if it's not hidden then make it green so we know it's been
            // selected by the filter
            newData.label = anonymize(data.label);
          } else {
            hidden = true;

            sigmaGraph.neighbors(node).forEach(n => {
              var label = anonymize(sigmaGraph.getNodeAttributes(n).label).toLowerCase();

              if (filter.users.includes(label)) {

                console.log("neighbour of " + label + " is " + anonymize(data.label));

                var edge = sigmaGraph.edge(node, n) || sigmaGraph.edge(n, node);

                var weight = sigmaGraph.getEdgeAttributes(edge).weight;

                hidden = hidden && (weight < range[0] || weight > range[1]);
              }
            })
          }
        }

        if (hoveredNode) {

          if (node !== hoveredNode && sigmaGraph.neighbors(hoveredNode).includes(node)) {

            var edge = sigmaGraph.edge(node, hoveredNode) || sigmaGraph.edge(hoveredNode, node);

            var weight = sigmaGraph.getEdgeAttributes(edge).weight;

            if (weight >= range[0] && weight <= range[1]) {
              newData.label = anonymize(data.label);
            }
          }

          if (node === hoveredNode || (sigmaGraph.neighbors(hoveredNode).includes(node) && !hidden)) {
            // if the node being rendered is either being hovered over or is a
            // neightbour of that node then


            if (node === hoveredNode) {
              newData.label = anonymize(data.label);
            } else {
              var edge = sigmaGraph.edge(node, hoveredNode) || sigmaGraph.edge(hoveredNode, node);

              var weight = sigmaGraph.getEdgeAttributes(edge).weight;
              if (weight >= range[0] && weight <= range[1]) {
                newData.label = anonymize(data.label);
              }
            }
          }
        }

        newData.hidden = hidden;

        return newData;
      },
      edgeReducer: (edge, data) => {
        const sigmaGraph = sigma.getGraph();
        const newData = { ...data, hidden: false };

        newData.color = sigmaGraph.getEdgeAttributes(edge).color;

        const sourceAttributes = sigmaGraph.getNodeAttributes(sigmaGraph.source(edge));
        const targetAttributes = sigmaGraph.getNodeAttributes(sigmaGraph.target(edge));

        if (displayType === "communities") {
          if (sourceAttributes.community === targetAttributes.community) {
            newData.color = getUniqueColor(sourceAttributes.community);
          } else {
            newData.color = "#E6E6E6";
            //newData.hidden = true;
          }
        }

        const weight = sigmaGraph.getEdgeAttributes(edge).weight;

        if (weight < range[0] || weight > range[1]) {
          newData.hidden = true;
        } else if (hoveredNode && !draggedNode && !sigmaGraph.extremities(edge).includes(hoveredNode)) {
          newData.hidden = true;
        } else if (clickedEdge) {
          if (edge === clickedEdge) {
            newData.color = "red";
          }
        }

        if (filter !== null && filter.hashtags.length > 0) {
          var matches = 0;

          sigmaGraph.getEdgeAttributes(edge).hashtags.forEach(hashtag => {
            if (filter.hashtags.includes(hashtag.toLowerCase()))
              ++matches;
          });

          newData.hidden = !(filter.hashtagMode === "any" ? matches !== 0 : matches === filter.hashtags.length);
        }

        //newData.color = "#E6E6E6";

        return newData;
      },
    });

  }, [sigma, hoveredNode, draggedNode, clickedNode, clickedEdge, range, filter, setSettings, displayType, anonymize]);
  return null;
};


export const ResultDisplay = (props) => {

    const { anonymize, graph, jobID, data } = props;

    const [clickedNode, setClickedNode] = useState(null);
    const [clickedEdge, setClickedEdge] = useState(null);


    const [filter, setFilter] = React.useState(null);

    const [users, setUsers] = React.useState("");
    const [hashtags, setHashtags] = React.useState("");
    const [hashtagMode, setHashtagMode] = React.useState("any");

    const [range, setRange] = React.useState([1 / 3, 1]);

    const handleRangeChange = (event, newValue) => {
        setRange(newValue);
    };

    const addToFilter = (type, value) => {
        if (type === "user") {
            setUsers(users + " " + value.text);
        } else if (type === "hashtag") {
            setHashtags(hashtags + " " + value.text);
        }
    };

    const filterData = () => {

        setClickedNode(null);
        setClickedEdge(null);

        if (users.trim() === "" && hashtags.trim() === "") {
            setFilter(null);
            return;
        }

        setFilter({
            users: users.trim() === "" ? [] : users.trim().toLowerCase().split(/\s+/),
            hashtags: hashtags.trim() === "" ? [] : hashtags.trim().toLowerCase().split(/\s+/),
            hashtagMode: hashtagMode
        });
    }

    const resetFilter = () => {

        setUsers("");
        setHashtags("");
        setHashtagMode("any");
        setFilter(null);
    }

    const [displayType, setDisplayType] = useState("weight");

    const handleDisplayType = (event, newType) => {
        if (newType !== null) {
            setDisplayType(newType);
        }
    };

    const marks = []
    for (var i = 0.1 ; i < 1 ; i = i+0.1) {
        marks.push({
            value: i
        })
    }

    return (
        <Box mt={3}>
            <Grid container direction={"column"} component={Paper}>
                <Grid container direction={"row"} alignItems="center" spacing={3}>
                    <Grid item xs={7}>
                        <TextField
                            id="users"
                            label="users"
                            variant="filled"
                            value={users}
                            sx={{ width: "100%" }}
                            onChange={(event) => {
                                setUsers(event.target.value);
                            }} />
                    </Grid>
                    <Grid item xs={1}>
                        <Button variant="contained" color="primary" onClick={() => filterData()}>Filter</Button>
                    </Grid>
                    <Grid item xs={4}>
                        <Box sx={{ paddingLeft: "3em", paddingRight: "3em" }}>
                            <Typography gutterBottom>Showing Edges with Weights between {range[0].toFixed(2)} and {range[1].toFixed(2)}</Typography>
                            <Slider
                                value={range}
                                step={0.01}
                                min={0}
                                max={1}
                                marks={marks}
                                onChange={handleRangeChange} />
                        </Box>
                    </Grid>
                </Grid>
                <Grid container direction={"row"} alignItems="center" spacing={3}>
                    <Grid item xs={6}>
                        <TextField
                            id="hashtags"
                            label="links"
                            variant="filled"
                            value={hashtags}
                            sx={{ width: "100%" }}
                            onChange={(event) => {
                                setHashtags(event.target.value);
                            }} />
                    </Grid>
                    <Grid item xs={1}>
                        <Select
                            id="hashtagMode"
                            value={hashtagMode}
                            label="Mode"
                            onChange={(event) => {
                                setHashtagMode(event.target.value)
                            }} >
                            <MenuItem value={"any"}>Any</MenuItem>
                            <MenuItem value={"all"}>All</MenuItem>
                        </Select>
                    </Grid>
                    <Grid item xs={1}>
                        <Button variant="contained" color="primary" onClick={() => resetFilter()}>Reset</Button>
                    </Grid>
                </Grid>

                <Box mt={3} />

                <Tabs defaultValue={"overview"}>
                    <TabList>
                        <Tab value={"overview"}><BubbleChartIcon color="action"> </BubbleChartIcon>Visualization</Tab>
                        {data.posts && <Tab value={"times"}><ScheduleIcon color="action" /> Post Times</Tab>}
                        {data.posts && <Tab value={"links"}><LinkIcon color="action"/> Link Times</Tab>}
                        {data.nodes[0]["attributes"]["created_at"] && <Tab value={"age"}><PersonIcon color="action"/> Account Age</Tab>}
                        <Tab value={"edges"}><TableChartIcon color="action" /> Raw Data</Tab>
                    </TabList>

                    <TabPanel value={"overview"}>

                        <Grid container direction={"row"} style={{background:"white"}}>
                            <Grid item xs={7}>
                                <SigmaContainer style={{ height: "500px", width: "100%" }}>
                                    <ControlsContainer position={"bottom-left"}>
                                        <ZoomControl />
                                    </ControlsContainer>
                                    <LoadGraph
                                        anonymize={anonymize}
                                        graph={graph}
                                        clickedNode={clickedNode}
                                        setClickedNode={setClickedNode}
                                        clickedEdge={clickedEdge}
                                        setClickedEdge={setClickedEdge}
                                        range={range}
                                        filter={filter}
                                        displayType={displayType}
                                    />
                                </SigmaContainer>
                            </Grid>

                            <Grid item xs={5}>
                                <NodeDisplay key={clickedNode} anonymize={anonymize} node={clickedNode} graph={graph} addToFilter={addToFilter} jobID={jobID} data={data} />
                                <EdgeDisplay key={clickedEdge} anonymize={anonymize} edge={clickedEdge} graph={graph} posts={data.posts} addToFilter={addToFilter} jobID={jobID} />
                                {clickedNode === null && clickedEdge === null && 
                                    <div style={{padding:"1em"}}>
                                        <Typography paragraph variant={"body"}>Each node in the graph represents a single account. The size of the node represents the number of other nodes it has links with. This usually means that an account represented by a larger node has made more posts, but it does not necessarily mean that they are involved in coordinated dissemination.</Typography>
                                        <Typography paragraph variant={"body"}>Coordination is shown via the edges in the graph. Each edge suggests some level of coordination between the two nodes it connects. The darker and thicker the edge the stronger the likelihood they are working together in a coordinated way. The edges are also used to assign each node to a community (using the standard <Link href="https://en.wikipedia.org/wiki/Louvain_method" target="_blank">Louvain algorithm</Link> and the nodes are then coloured and grouped by community.</Typography>
                                        <Typography paragraph variant={"body"}>Click on a node or edge for more details. To move a node hold down shift and then drag the node to its new position.</Typography>
                                    </div>}
                            </Grid>

                        </Grid>
                    </TabPanel>

                    <TabPanel value={"edges"}>
                        <RawEdgeDisplay anonymize={anonymize} graph={graph} filter={filter} range={range} />
                    </TabPanel>

                    <TabPanel value={"times"}>
                        <PostTimes anonymize={anonymize} graph={graph} filter={filter} range={range} data={data} />
                    </TabPanel>

                    <TabPanel value={"links"}>
                        <LinkTimes anonymize={anonymize} graph={graph} filter={filter} range={range} data={data} />
                    </TabPanel>

                    <TabPanel value={"age"}>
                        <AccountAge anonymize={anonymize} graph={graph} filter={filter} range={range} data={data} />
                    </TabPanel>
                </Tabs>

            </Grid>
        </Box>
    );
};
