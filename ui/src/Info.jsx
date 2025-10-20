import React from "react";
import { Typography } from "@mui/material"

export const Info = (props) => {

    return (
        <React.Fragment>
            <Typography variant={"body2"} sx={{ margin: "2em" }}>
                One tactic employed by disinformation campaigns to amplify their influence and impact
                to manipulate public opinion is utilizing organized coordination, where a group of
                accounts collaborates to disseminate specific beliefs and opinions. Therefore, identifying
                organized coordination in online social networks is a concern. Our model focuses on
                identifying coordination within online social networks by analyzing how accounts share
                common hashtags. By examining the frequency and timing of hashtag sharing, the model aims
                to uncover anomalies in accounts' sharing behavior. It comprises three main steps: identifying
                suspicious accounts with heightened sharing activity, determining coordination levels between
                pairs of accounts based on shared hashtags, and adjusting coordination levels to distinguish
                between organized and unorganized coordination. This hierarchical approach enables the
                detection of anomalies across individual accounts, pairs of accounts, and groups, without
                relying on account interactions or predefined thresholds, thus enhancing its practical
                applicability.</Typography>

            <Typography variant={"body2"} sx={{ margin: "2em" }}>The model assigns a value within the range
                of [0,1] to determine the coordination level between every pair of accounts, with higher values
                indicating stronger coordination. Furthermore, for each pair of coordinated accounts, the model
                calculates a value within [0,1] for every hashtag shared in a coordinated manner, indicating the
                level of effort exerted to propagate the hashtag. This systematic approach facilitates the
                identification of hashtags that are coordinatedly disseminated with greater intensity, providing
                valuable insights into coordinated disinformation campaigns in online social
                networks.</Typography>
        </React.Fragment>
    )

}