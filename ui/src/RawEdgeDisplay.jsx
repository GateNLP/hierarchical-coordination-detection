import React, { memo } from "react";
import Table from '@mui/material/Table';
import TableBody from '@mui/material/TableBody';
import TableCell from '@mui/material/TableCell';
import TableContainer from '@mui/material/TableContainer';
import TableHead from '@mui/material/TableHead';
import TableRow from '@mui/material/TableRow';
import Paper from '@mui/material/Paper';
import { Typography } from "@mui/material";

import Chip from '@mui/material/Chip';

export const RawEdgeDisplay = memo(function RawEdgeDisplay(props) {
    const { anonymize, graph, filter, range } = props;

    if (graph === null) return null;

    return (
        <React.Fragment>
            <Typography variant={"body2"} paragraph>This table shows the raw data behind the currently filtered edges. Note that the names of the users
                are coloured to match the communuity they belong to; i.e. the color of their name in this table will match the color of their node in
                the main network graph. This makes it easy to spot at a glance edges which are between two nodes in a community (the users have the same
                color) or that link two communities together (the users have different colors).</Typography>
            <TableContainer component={Paper} sx={{ maxHeight: 440 }}>
                

                <Table stickyHeader size='small'>
                    <TableHead>
                        <TableRow>
                            <TableCell>User A</TableCell>
                            <TableCell>User B</TableCell>
                            <TableCell>Weight</TableCell>
                            <TableCell>Links</TableCell>
                        </TableRow>
                    </TableHead>
                    <TableBody>
                        {
                            
                            
                            graph.edges().slice().reverse().map((edge) => {

                                var weight = graph.getEdgeAttributes(edge).weight;

                                if (weight < range[0] || weight > range[1]) return null;

                                var source = graph.getNodeAttributes(graph.source(edge)).label;
                                var target = graph.getNodeAttributes(graph.target(edge)).label;
                                var hashtags = graph.getEdgeAttributes(edge).hashtags.join(", ");

                                var display = filter === null || filter.users.length === 0 ||
                                    filter.users.includes(anonymize(source).toLowerCase()) ||
                                    filter.users.includes(anonymize(target).toLowerCase());

                                if (display && filter !== null && filter.hashtags.length > 0) {
                                    var matches = 0;

                                    graph.getEdgeAttributes(edge).hashtags.forEach(hashtag => {
                                        if (filter.hashtags.includes(hashtag.toLowerCase()))
                                            ++matches;
                                    });

                                    display = (filter.hashtagMode === "any" ? matches !== 0 : matches === filter.hashtags.length);

                                }

                                if (display) {
                                    const sourceColour = graph.getNodeAttributes(graph.source(edge)).color;
                                    const targetColour = graph.getNodeAttributes(graph.target(edge)).color;
                                    return (
                                        <TableRow key={edge}>
                                            <TableCell ><Chip sx={(theme) => ({backgroundColor: sourceColour, color: theme.palette.getContrastText(sourceColour)})} label={anonymize(source)}/></TableCell>
                                            <TableCell ><Chip sx={(theme) => ({backgroundColor: targetColour, color: theme.palette.getContrastText(targetColour)})} label={anonymize(target)}/></TableCell>
                                            <TableCell>{weight.toFixed(4)}</TableCell>
                                            <TableCell>{hashtags}</TableCell>
                                        </TableRow>)
                                }

                                return null;
                            })
                        }
                    </TableBody>
                </Table>
            </TableContainer>
        </React.Fragment>
    )
})