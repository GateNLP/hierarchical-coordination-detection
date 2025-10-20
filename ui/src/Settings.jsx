import { Card, CardContent, IconButton } from '@mui/material/';
import Typography from '@mui/material/Typography';
import Grid from '@mui/material/Grid';
import Radio from '@mui/material/Radio';
import Box from '@mui/material/Box';
import Switch from '@mui/material/Switch';
import TextField from '@mui/material/TextField';
import FormControlLabel from '@mui/material/FormControlLabel';


export const Settings = (props) => {
    const { settings, setSettings } = props;

    function updateSetting(setting, value) {
        var updated = { ...settings }
        updated[setting] = value;

        setSettings(updated);
    }

    const changeSpeed = (event) => {
        updateSetting("speed", event.target.value);
        //setDataSource(event.target.value)
    }

    return (
        <Box mt={3}>
            <Grid

                container
                direction="row"
                spacing={3}
                alignItems="flex-start"
                data-cy="settingsImdt"
            >

                <Grid item xs={12} >
                    <FormControlLabel
                        control={
                            <Switch id="setAnonymous"
                                checked={settings.anonymous}
                                onChange={(e) => {
                                    updateSetting("anonymous", !settings.anonymous);
                                }}
                            />
                        }
                        sx={{ml:0}}
                        labelPlacement="start"
                        label={<Typography>Anonymous Mode:</Typography>}
                    />
                    <Typography variant={"body1"} paragraph>By default we anonymous the names of users within the dashboard; this means it is safe to demonstrate to others by default. Turning this option off allows you to see the real names should you need to.</Typography>
                </Grid>

                <Grid item xs={12} data-cy="settingsImdtHeader">
                    <Typography variant={"h6"} style={{ paddingBottom: 3 }}>
                        Changes to the following settings only take affect after you re-process the input file.
                    </Typography>
                </Grid>

                <Grid item xs={12}>
                    <Typography>Algorithm Complexity:</Typography>
                    <Typography variant={"body1"} paragraph>Calcualting coordination on large datasets can take a long time. Whilst we default
                    to running the full algorithm, you can choose to run a simpler approach if required.

                    <Typography varaint={"body1"}><Radio checked={settings.speed === 1} value={1} onChange={changeSpeed} /> Shared The Same Links (Simplest)</Typography>
                    <Typography varaint={"body1"}><Radio checked={settings.speed === 2} value={2} onChange={changeSpeed} /> Pairwise Level Coordination</Typography>
                    <Typography varaint={"body1"}><Radio checked={settings.speed === 3} value={3} onChange={changeSpeed} /> Pairwise and Group Level Coordination (Default)</Typography>
                    </Typography>
                </Grid>

                <Grid item xs={12} >
                    <Typography>Exclude Common Links:</Typography>
                    <Typography variant={"body1"} paragraph>Depending upon the dataset being analysed it may be beneficial to exclude certain
                    common links from the analysis. For example, if using hashtags to relate posts in a COVID-19 dataset it is unlikely #covid19 would be
                    a sign of coordination.</Typography>
                    <Typography variant={"body1"}>Enter the linking entities you wish to exclude from the analysis; either one
                    per line or separated by a space.</Typography>
                    <TextField variant="outlined"
                        fullWidth
                        multiline
                        rows={3}
                        value={settings.exclude}
                        onChange={(e) => {
                            updateSetting("exclude",e.target.value);
                        }}
                    />
                </Grid>
            </Grid>
        </Box>
    )
}
