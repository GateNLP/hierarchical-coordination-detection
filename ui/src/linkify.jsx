import Link from "@mui/material/Link";
import React from "react";
import Box from '@mui/material/Box';
import styled from "@mui/material/styles/styled";

import processString from "react-process-string";

const LinkifiedBox = styled(Box)({
    "& > p:first-child": {
        marginTop: 0
    },
    "& > p:last-child": {
        marginBottom: 0
    }
});

function getAccountLink(platform, handle) {
    if (platform === "Twitter")
    return `https://twitter.com/${handle}`;

  if (platform === "Telegram") {
    return `https://t.me/${handle}`;
  }

  if (platform === "YouTube") {
    return `https://youtube.com/@${handle}`;
  }

  if (platform === "Mastodon") {
      var parts = handle.split("@");
      return `https://${parts[1]}/users/${parts[0]}`;
  }

  return "#";
}

const genericUrl = {
    regex: /(https?:\/\/(www\.)?[a-zA-Z0-9@:%._+~#=]{1,256}\.[a-z]{2,6}\b(?:[-a-zA-Z0-9%_+.~#?&@\/=]*[a-zA-Z0-9%_+~#@\/=])?)/gim, //regex to match a URL
    fn: (key, result) => {
        let url = result[1];

        return (
            <Link key={key} href={url} target="_blank">
                {url}
            </Link>
        );
    },
};

// replace line break with <br> - consecutive line breaks will already have been
// converted to paragraphs
const lineBreak = {
    regex: /\n+/g, // regex to match any remaining newlines
    fn: (key, result) => <br/>,
};


const configs = {
    Twitter: [
        {
            regex: /(?<=^|\s)@([a-zA-Z0-9_]+)/gim, //regex to match a username
            fn: (key, result) => {
                let username = result[1];

                return (
                    <Link
                        key={key}
                        href={getAccountLink('Twitter', username)}
                        target="_blank"
                    >
                        @{username}
                    </Link>
                );
            },
        },
        {
            regex: /(?<=^|\s)[#\uFF03]([\p{L}\p{N}_]+)/gimu, //regex to match a hashtag
            fn: (key, result) => {
                let hashtag = result[1];

                return (
                    <Link
                        key={key}
                        href={`https://twitter.com/hashtag/${hashtag}?f=live`}
                        target="_blank"
                    >
                        #{hashtag}
                    </Link>
                );
            },
        },
        genericUrl,
        lineBreak,
    ],

    Telegram: [
        {
            regex: /(?<=^|\s)@([a-zA-Z0-9_]+)/gim, //regex to match a username
            fn: (key, result) => {
                let username = result[1];

                return (
                    <Link
                        key={key}
                        href={getAccountLink('Telegram', username)}
                        target="_blank"
                    >
                        @{username}
                    </Link>
                );
            },
        },
        // telegram doesn't have a way to search globally for hashtags - even if you
        // do a "global search" in your telegram client it only finds matches within the channels
        // and groups that you are subscribed to
        genericUrl,
        lineBreak,
    ],

    YouTube: [
        {
            // YouTube usernames can include dots and dashes, their guidelines are not clear about
            // whether they can _end_ with a dot or dash but I've disallowed that in this regex so
            // it doesn't get confused by a username at the end of a sentence.
            regex: /(?<=^|\s)@([a-zA-Z0-9_.-]*[a-zA-Z0-9_])/gim,
            fn: (key, result) => {
                let username = result[1];

                return (
                    <Link
                        key={key}
                        href={getAccountLink('YouTube', username)}
                        target="_blank"
                    >
                        @{username}
                    </Link>
                );
            },
        },
        {
            regex: /(?<=^|\s)[#\uFF03]([\p{L}\p{N}_]+)/gimu, //regex to match a hashtag
            fn: (key, result) => {
                let hashtag = result[1];

                return (
                    <Link
                        key={key}
                        href={`https://youtube.com/hashtag/${hashtag}`}
                        target="_blank"
                    >
                        #{hashtag}
                    </Link>
                );
            },
        },
        genericUrl,
        lineBreak,
    ],

    "Mastodon": [
        {
            //regex to match a username including server (@someone@mas.to) - the username part must be letters,
            // numbers and underscore only, the server part can have dots and hyphens as well as it's a
            // normal internet hostname.
            regex: /(?<=^|\s)@([a-zA-Z0-9_]+@[a-zA-Z0-9._-]+)/gim,
            fn: (key, result) => {
                let username = result[1];

                return (
                    <Link
                        key={key}
                        href={getAccountLink('Mastodon', username)}
                        target="_blank"
                    >
                        @{username}
                    </Link>
                );
            },
        },
        {
            regex: /(?<=^|\s)[#\uFF03]([\p{L}\p{N}_]+)/gimu, //regex to match a hashtag
            fn: (key, result) => {
                let hashtag = result[1];

                // for the moment all hashtags link to mastodon.social - should the server
                // be a parameter instead, so we link to the relevant server for a user?
                return (
                    <Link
                        key={key}
                        href={`https://mastodon.social/tags/${hashtag}`}
                        target="_blank"
                    >
                        #{hashtag}
                    </Link>
                );
            },
        },
        genericUrl,
        lineBreak,
    ]
};

export function withLinks(text, platform = "Twitter") {

    if (text === undefined || text === null) return "";

    // this ensures that any HTML entities (like &amp;) are safely
    // expanded back to their original versions
    text = (new DOMParser().parseFromString(text, "text/html")).documentElement.textContent;
  
    const platformConfig = configs[platform];
    if(platformConfig) {
        const processFn = processString(platformConfig);
        return (
            // first split into paragraphs at two-or-more newlines, then process each paragraph
            <LinkifiedBox>
                {text.trim().split(/\n\n+/g).map(para => <p>{processFn(para)}</p>)}
            </LinkifiedBox>
        );
    } else {
        return text;
    }
}