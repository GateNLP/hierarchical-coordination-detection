import React, { useEffect } from "react";
import axios from "axios";

import { Card, CardContent, IconButton } from '@mui/material/';
import Typography from '@mui/material/Typography';

import CommentIcon from '@mui/icons-material/Comment';
import PersonIcon from '@mui/icons-material/Person';

import RepeatIcon from '@mui/icons-material/Repeat';
import FormatQuoteIcon from '@mui/icons-material/FormatQuote';
import ReplyIcon from '@mui/icons-material/Reply';
import ChatBubbleOutlineIcon from '@mui/icons-material/ChatBubbleOutline';

import { withLinks } from "./linkify";

export const PostDisplay = (props) => {
    const { anonymize, post, sx, jobID } = props;

    const [data, setData] = React.useState(null);


    if (typeof post !== 'object' && post !== null) {
        useEffect(() => {
        
            axios.get(
                `./posts/${jobID}?id=${encodeURIComponent(post)}`
            )
            .then((response) => {
    
                //console.log(response.data);
                setData(response.data);
    
            }, (error) => {
                // for now just log the error to the console
                console.log(error);
            });
    
          }, [post]); // <- add the count variable here



          if (data === null) {
            return (
                <Card variant="outlined" sx={{ m: "5px", ...sx }}>
                    <CardContent><Typography variant="body2">Loading post information...</Typography></CardContent>
                </Card>
            )
          }

        var style = {fontSize:"small", verticalAlign:"baseline"}

          return (
            <Card variant="outlined" sx={{ m: "5px", ...sx }}>
                <CardContent>
                    <Typography sx={{ fontSize: 14 }} color="text.secondary">{data["timestamp"]} UTC{!anonymize("_SETTING") && <IconButton sx={{ float: "right" }} color="primary" target="_blank" href={"https://twitter.com/" + data["screenName"] + "/status/" + data["postId"]} variant="small"><CommentIcon color="primary" sx={{ fontSize: "small" }} /></IconButton>}{!anonymize("_SETTING") && <IconButton sx={{ float: "right" }} color="primary" target="_blank" href={"https://twitter.com/" + data["screenName"]} variant="small"><PersonIcon color="primary" sx={{ fontSize: "small" }} /></IconButton>}</Typography>
                    <Typography variant="body2">{withLinks(data["text"])}</Typography>
                </CardContent>
            </Card>
          )


    
    } else {
        

        var style = {fontSize:"small", verticalAlign:"baseline"}

        return (
            <Card variant="outlined" sx={{ m: "5px", ...sx }}>
                <CardContent>
                    <Typography sx={{ fontSize: 14 }} color="text.secondary">{post.time} UTC{!anonymize("_SETTING") && <IconButton sx={{ float: "right" }} color="primary" target="_blank" href={"https://twitter.com/" + post.user + "/status/" + post.id} variant="small"><CommentIcon color="primary" sx={{ fontSize: "small" }} /></IconButton>}{!anonymize("_SETTING") && <IconButton sx={{ float: "right" }} color="primary" target="_blank" href={"https://twitter.com/" + post.user} variant="small"><PersonIcon color="primary" sx={{ fontSize: "small" }} /></IconButton>}</Typography>
                    <Typography variant="body2">{withLinks(post.text)}</Typography>
                </CardContent>
            </Card>)

    }
}