import { Box } from '@mui/material/';
import Typography from '@mui/material/Typography';

// formatting code is heavily based on this jsfiddle
// https://jsfiddle.net/MrPolywhirl/0gyxp4ck/

const
    MS_DAYS = 8.64e7,
    MS_HOURS = 3.6e6,
    MS_MINUTES = 6e4,
    MS_SECONDS = 1e3;

const defaultDuration = { days: 0, hours: 0, minutes: 0, seconds: 0 };

const pluralize = (text, count = 1, suffix = 's') =>
    count === 1 ? text : `${text}${suffix}`

const divmod = (n, m) => [Math.trunc(n / m), n % m];

const fromMillis = (durationMs) => {
    const [days, daysMs] = divmod(durationMs, MS_DAYS);
    const [hours, hoursMs] = divmod(daysMs, MS_HOURS);
    const [minutes, minutesMs] = divmod(hoursMs, MS_MINUTES);
    const seconds = minutesMs / MS_SECONDS;
    return { days, hours, minutes, seconds };
};

const formatDuration = (duration, includeAll) => {
    const d = { ...defaultDuration, ...duration };
    return [
        { count: d.days, text: `${d.days} ${pluralize('day', d.days)}` },
        { count: d.hours, text: `${d.hours} ${pluralize('hour', d.hours)}` },
        { count: d.minutes, text: `${d.minutes} ${pluralize('minute', d.minutes)}` },
        { count: d.seconds, text: `${d.seconds} ${pluralize('second', d.seconds)}` }
    ]
        .filter(({ count }) => includeAll || count > 0)
        .map(({ text }) => text)
        .join(' ');
};

export const TimeGap = (props) => {
    const { posts, post1, post2 } = props;

    var t1 = posts[post1].time;
    var t2 = posts[post2].time;

    // the difference in milliseconds between the two posts
    var diff = Math.abs(t1-t2)

    if (diff > 0)
        // if there is a difference in time then display it nicely formatted
        return (
            <Box sx={{ m: "5px", textAlign: "center" }}>
                <Typography color="text.secondary" variant="body2">{formatDuration(fromMillis(diff))} later</Typography>
            </Box>)

    // if there is no difference then don't add anything to the DOM
    return null;    
}