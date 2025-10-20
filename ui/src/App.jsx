import './App.css';
import { ResultDisplay } from './ResultDisplay';
import React, { useCallback, useState, useEffect, useRef } from "react";
import axios from "axios";
import { scaleSqrt, scaleLinear } from "d3-scale";
import { interpolateGreys } from "d3-scale-chromatic";

import UndirectedGraph from "graphology";
import circlepack from 'graphology-layout/circlepack';
import { Grid, Typography, Link, Button, IconButton, CircularProgress, TextField, Dialog, DialogContent, Radio, Select, MenuItem, FormControl, InputLabel } from '@mui/material';
import UoSLogo from "./images/UoS_Crest.svg?react"
import VigilantLogo from "./images/vigilant.svg?react"
import { Info } from "./Info"

import CloudUploadIcon from '@mui/icons-material/CloudUpload';
import CloudDownloadIcon from '@mui/icons-material/CloudDownload';

import SettingsIcon from '@mui/icons-material/Settings';

import Papa from "papaparse";

import * as SparkMD5 from 'spark-md5';

import { Settings } from "./Settings"

const animals = ["Alligator", "Anteater", "Armadillo", "Aurochs", "Axolotl", "Badger", "Bat", "Beaver", "Buffalo", "Camel", "Capybara", "Chameleon", "Cheetah", "Chinchilla", "Chipmunk", "Chupacabra", "Cormorant", "Coyote", "Crow", "Dingo", "Dinosaur", "Dolphin", "Duck", "Elephant", "Ferret", "Fox", "Frog", "Giraffe", "Gopher", "Grizzly", "Hedgehog", "Hippo", "Hyena", "Ibex", "Ifrit", "Iguana", "Jackal", "Jackalope", "Kangaroo", "Koala", "Kraken", "Lemur", "Leopard", "Liger", "Llama", "Manatee", "Mink", "Monkey", "Moose", "Narwhal", "Nyan Cat", "Orangutan", "Otter", "Panda", "Penguin", "Platypus", "Pumpkin", "Python", "Quagga", "Rabbit", "Raccoon", "Rhino", "Sheep", "Shrew", "Skunk", "Slow Loris", "Squirrel", "Tiger", "Turtle", "Walrus", "Wolf", "Wolverine", "Wombat"];

const randSeed = Math.random() * 4294967296;

export const getUniqueColor = (n) => {
  const rgb = [0, 0, 0];

  for (let i = 0; i < 24; i++) {
    rgb[i % 3] <<= 1;
    rgb[i % 3] |= n & 0x01;
    n >>= 1;
  }

  return '#' + rgb.reduce((a, c) => (c > 0x0f ? c.toString(16) : '0' + c.toString(16)) + a, '')
};

var embeddedJobId = (new URLSearchParams(window.location.search)).get("jobid");
var embedded = !!embeddedJobId

function App() {

  const [datasets, setDatasets] = useState(null);

  useEffect(() => {
    fetch("./jobs/examples")
    .then(res => {
        if(res.ok) {
            res.json().then(data => {
                if(data && data.length) {
                    data.sort((a,b) => (a.label > b.label) ? 1 : ((b.label > a.label) ? -1 : 0))
                    setDatasets(data)
                } else {
                    console.log("No sample datasets available");
                }
            })
        } else {
            res.text().then(msg => console.error(`Could not retrieve datasets: ${msg}`));
        }
    }).catch(reason => {
        console.error("Error retrieving samples:", reason);
    });
  }, []);

  const [file, setFile] = useState(null);

  const [data, setData] = useState(null);

  const [jobID, setJobID] = useState(embedded ? embeddedJobId : null);

  const [hashtags, setHashtags] = React.useState("");

  const [dataSource, setDataSource] = React.useState(embedded ? 'es' : 'csv');

  const [toAnimals, setToAnimals] = useState(null);
  const [fromAnimals, setFromAnimals] = useState(null);

  const [dataset, setDataset] = useState(0);
  const handleDataset = (event) => {
    setDataset(parseInt(event.target.value));
  };

  const changeDatasource = (event) => {
    setDataSource(event.target.value)
  }

  // eslint-disable-next-line
  const [graph, setGraph] = useState(new UndirectedGraph());

  const [processing, setProcessing] = useState(embedded);
  const [upload, setUpload] = useState(-1);
  const [log, setLog] = useState("");

  var intervalId = useRef(-1);

  useEffect(() => {
    // start polling immediately on mount if we are in embedded mode.
    if (embedded && intervalId.current === -1) {
      // queue up the polling loop
      intervalId.current = window.setInterval(poll, 5000, embeddedJobId);

      // and fire one immediate poll now without the 5 second delay
      poll(embeddedJobId);
    }
  }, []);

  const [settings, setSettings] = useState({
    speed: 3,
    exclude: "",
    anonymous: !embedded
  })

  const [openSettings, setOpenSettings] = useState(false);

  const handleOpenSettings = (e) => {
    //e.preventDefault();
    setOpenSettings(true);
  }

  const handleCloseSettings = () => {
    setOpenSettings(false);
  }

  async function getJobStatus(jobId) {
    let response = await fetch(`./jobs/${jobId}`);
    var json = await response.json();

    return json;
  }

  const anonymize = useCallback((str, mode = true) => {

    if (str === "_SETTING") return settings.anonymous;

    if (!settings.anonymous) return str;

    var result = mode ? toAnimals[str] : fromAnimals[str];

    return result;
  }, [settings.anonymous, toAnimals, fromAnimals]);

  function computeChecksumMd5(file) {
    // code is based on this typescript example
    // https://dev.to/qortex/compute-md5-checksum-for-a-file-in-typescript-59a4

    return new Promise((resolve, reject) => {
      const chunkSize = 2097152; // Read in chunks of 2MB
      const spark = new SparkMD5.ArrayBuffer();
      const fileReader = new FileReader();

      let cursor = 0; // current cursor in file

      fileReader.onerror = function () {
        reject('MD5 computation failed - error reading the file');
      };

      // read chunk starting at `cursor` into memory
      function processChunk(chunk_start) {
        const chunk_end = Math.min(file.size, chunk_start + chunkSize);
        fileReader.readAsArrayBuffer(file.slice(chunk_start, chunk_end));
      }

      // when it's available in memory, process it
      fileReader.onload = function (e) {
        spark.append(e.target.result); // Accumulate chunk to md5 computation
        cursor += chunkSize; // Move past this chunk

        if (cursor < file.size) {
          // Enqueue next chunk to be accumulated
          processChunk(cursor);
        } else {
          // Computation ended, last chunk has been processed. Return as Promise value.
          resolve(spark.end());
        }
      };

      processChunk(0);
    });
  }

  /**
   * One iteration of the polling cycle to check whether a job is complete
   * and process the results if/when it finishes.
   * @param jobId {string} the job ID
   * @param postsPromise {Promise<{[p: id]: any}> | undefined} an optional promise that will
   *        resolve to the map from post ID to post details.  For elasticsearch
   *        jobs this will be undefined since the post data is already in the job
   *        results JSON, but for CSV jobs we have to build this structure
   *        ourselves from the source CSV file.
   */
  async function poll(jobId, postsPromise) {

    var json = await getJobStatus(jobId);

    setLog("Job with ID " + json.jobID + " has status: " + json.status);

    if (json.status === "finished") {
      clearInterval(intervalId.current)

      setLog("Collecting result data...")

      try {
        const response = await axios.get(`./jobs/${jobId}/graph`);

        setLog("Converting result data...")

        var raw = response.data;

        var edges = raw["edges"].reverse();

        var edgeMax = edges[edges.length - 1]["attributes"]["size"];
        var edgeMin = edges[0]["attributes"]["size"];
        //var scale = scaleSqrt().domain([edgeMin, edgeMax]).range([0.1, 3]);

        var linear = scaleLinear().domain([edgeMin, edgeMax]).range([0, 1]);


        edges.forEach(edge => {

          edge.attributes.weight = linear(edge.attributes.size);
          edge.attributes.size = linear(edge.attributes.size) * 5;
          edge.attributes.color = interpolateGreys(edge.attributes.weight * 0.6);

        })

        graph.import(raw);

        const communityHashtagsData = {}

        graph.forEachEdge(edge => {
            const sc = graph.getNodeAttributes(graph.source(edge)).community;
            const tc = graph.getNodeAttributes(graph.target(edge)).community;

            if (sc === tc ) {

                if (!communityHashtagsData[sc]) communityHashtagsData[sc] = {};

                const hashtags = graph.getEdgeAttributes(edge).hashtags;
                hashtags.forEach(hashtag => {
                  communityHashtagsData[sc][hashtag] = 1 + (hashtag in communityHashtagsData ? communityHashtagsData[sc][hashtag] : 0);
                });
            }
        })

        const communityNodeData = {}

        var maxDegree = 0;
        graph.forEachNode((node, attributes) => {
          maxDegree = Math.max(maxDegree, graph.degree(node))

          if (!communityNodeData[attributes.community]) communityNodeData[attributes.community] = {};
          communityNodeData[attributes.community][attributes.label] = graph.degree(node);
        })

        var nodeLinear = scaleLinear().domain([1, maxDegree]).range([5, 20]);

        if (dataSource === "es") {
          const anon = {};
          const deanon = {}
          var count = 0;

          graph.forEachNode((node, attributes) => {
            var name = animals[count % animals.length] + "_" + (count + 1);
            anon[attributes.label] = name;
            deanon[name.toLowerCase()] = attributes.label;
            ++count;
          });

          setToAnimals(anon);
          setFromAnimals(deanon);
        }

        graph.forEachNode((node, attributes) => {
          attributes.size = nodeLinear(graph.degree(node));
          attributes.color = getUniqueColor(attributes.community);
        })

        circlepack.assign(graph, {
          hierarchyAttributes: ['community', 'degree']
        });

        raw.jobID = jobId;

        raw.communities = raw.communities || {}
        raw.communities["links"] = communityHashtagsData;
        raw.communities["nodes"] = communityNodeData;

        if(postsPromise) {
          try {
            raw.posts = await postsPromise;
          } catch(e) {
            console.error("Could not parse posts CSV", e);
            raw.posts = {};
          }
        }

        setData(raw);

        setProcessing(false);
      } catch(e) {
        console.error(e);
      }
    }
  }

  async function processDataset() {

    if (intervalId.current !== -1) clearInterval(intervalId.current);

    graph.clear();
    setData(null);

    setLog("Uploading dataset details...");

    setProcessing(true);
    setUpload(0);

    var config = datasets[dataset].config;

    // copy the config so that if we want to
    // modify it we can do
    config = {
      "elasticsearch": config.elasticsearch,
      "index": config.index,
      "link_type": config.link_type,
      "query": config.query,
      "speed": settings.speed
    }

    if (settings.exclude.trim() !== "") {
      var exclude = settings.exclude.trim().toLowerCase().split(/\s+/).sort();
      if (exclude.length > 0) config["ignore"] = exclude
    }

    if (hashtags !== "") {
      var terms = hashtags.toLowerCase().split(/\s+/)
      if (terms.length > 0) {
        config["query"] = {
          "bool": {
            "must": [
              config.query,
              {
                "terms": {
                  "hashtags.keyword": terms
                }
              }
            ]
          }
        }
      }
    }

    axios.post('./jobs/process', config, {
      onUploadProgress: (progressEvent) => {
        const percentCompleted = Math.round((progressEvent.loaded * 100) / progressEvent.total);
        setUpload(percentCompleted);
      }
    })
      .then(response => {

        setUpload(-1);

        var json = response.data

        setLog("Job with ID " + json.jobID + " has status: " + json.status);

        setJobID(json.jobID)

        if (json.status === "finished")
          poll(json.jobID);
        else {
          if(intervalId.current !== -1) {
            window.clearInterval(intervalId.current);
          }
          intervalId.current = window.setInterval(poll, 5000, json.jobID)
        }
      })
      .catch(error => {
        if(intervalId.current !== -1) {
          window.clearInterval(intervalId.current);
        }
        console.error(error);
      });
  }

  async function uploadFile() {

    if (intervalId.current !== -1) clearInterval(intervalId.current);

    graph.clear();
    setData(null);

    setLog("Uploading file...");

    setProcessing(true);
    setUpload(0);

    var rows = {}

    var users = new Set();

    const postsPromise = new Promise((resolve, reject) => {
      Papa.parse(file, {
        worker: true,
        header: true,
        step: function (results) {
          rows[results.data.Post_ID] = {
            text: results.data.Post_text,
            time: results.data.Post_time,
            user: results.data.Screen_Name,
            id: results.data.Post_ID
          };

          // not sure why this happens but filter it out now
          if (results.data.Screen_Name !== undefined)
            users.add(results.data.Screen_Name);
        },
        complete: function () {

          const anon = {};
          const deanon = {}
          var count = 0;

          users.forEach((user) => {
            var name = animals[count % animals.length] + "_" + (count + 1);
            anon[user] = name;
            deanon[name.toLowerCase()] = user;
            ++count;
          });

          /*Object.keys(rows).forEach((id)  => {
            rows[id].anonymous = anon[rows[id].user];
          })*/

          setToAnimals(anon);
          setFromAnimals(deanon);
          // pass on the rows so they can be inserted into the final data set
          resolve(rows);
        },
        error(error, file) {
          reject(error);
        }
      });
    });

    var options = "";

    options += settings.speed

    var hashtags = [];

    if (settings.exclude.trim() !== "") {
      hashtags = settings.exclude.trim().toLowerCase().split(/\s+/).sort();
    }

    const blob = new Blob([hashtags.join("\n")], { type: 'text/plain' })

    // the job ID is just the md5 hash of the uploaded CSV file so let's
    // calculate that now to see if we really do need to upload the file
    // or if it is already sitting on the server
    var jobId = await computeChecksumMd5(file) + "_" + options + "_" + await computeChecksumMd5(blob);

    setJobID(jobID)

    // get the status of this job, which may not exist of course
    var status = await getJobStatus(jobId);

    if (status.error === undefined) {
      // if there is no error response then we can assume the input file
      // has already been uploaded so we can then decide what to do based
      // on the status of the current job, just as we would after uploading
      // normally
      if (status.status === "finished")
        poll(jobId, postsPromise);
      else {
        if(intervalId.current !== -1) {
          clearInterval(intervalId.current);
        }
        intervalId.current = window.setInterval(poll, 5000, jobId, postsPromise)
      }

      return;
    }

    let formData = new FormData();
    formData.append("posts", file);


    formData.append('exclude', blob, 'exclude.csv')

    axios.post(`./jobs/process?options=${settings.speed}`, formData, {
      onUploadProgress: (progressEvent) => {
        const percentCompleted = Math.round((progressEvent.loaded * 100) / progressEvent.total);
        setUpload(percentCompleted);
      }
    })
      .then(response => {

        setUpload(-1);

        var json = response.data

        setLog("Job with ID " + json.jobID + " has status: " + json.status);

        if(intervalId.current !== -1) {
          clearInterval(intervalId.current);
        }
        intervalId.current = window.setInterval(poll, 5000, json.jobID, postsPromise)
      })
      .catch(error => {
        if(intervalId.current !== -1) {
          clearInterval(intervalId.current);
        }
        console.error(error);
      });
  }

  return (
    <div style={{ margin: "1em" }}>
      {!embedded && <Grid container alignItems={"center"} alignContent={"center"}>
        <Typography variant="h3" color={'primary'} style={{ flex: 1 }}>
          Coordination Detection
        </Typography>

        <Link href="https://gate-socmedia.group.shef.ac.uk/" target="_blank"><UoSLogo style={{ paddingRight: 40 }} /></Link>

        <Link href="https://www.vigilantproject.eu/" target="_blank"><VigilantLogo style={{ paddingRight: 40 }} /></Link>

      </Grid>}

      <Grid container direction={"row"} alignItems="center" spacing={3}>
        {!embedded && <Grid item xs={3}>
          <div>
            <Typography variant="body" paragraph>Upload your own Dataset for Processing:</Typography>
            <Radio checked={dataSource === 'csv'} value={"csv"} onChange={changeDatasource} />
            <TextField id="fileupload" type="file" disabled={dataSource !== "csv"}
              onChange={(event) => {
                setFile(event.target.files[0])
              }} /></div>

          <div style={{paddingTop:"1em"}}>
            <Typography variant="body" paragraph>Explore an Example Dataset:</Typography>
            <Radio checked={dataSource === 'es'} value={"es"} onChange={changeDatasource} disabled={!datasets} />
            <FormControl>
              <Select value={dataset} onChange={handleDataset} disabled={!datasets || dataSource !== "es"}>
                {datasets ? datasets.map((d, index) => (
                  <MenuItem key={index} value={index}>{d.label}</MenuItem>
                )) : (
                  <MenuItem value={0}>No example datasets available</MenuItem>
                )}
              </Select>
            </FormControl>
            <TextField id="filter"
              disabled={!datasets || dataSource !== "es"}
              value={hashtags}
              label={"Filter to posts containing these links"}
              onChange={(event) => {
                setHashtags(event.target.value.trim());
              }} />
          </div>


        </Grid>}

        <Grid item xs={2}>
          {!processing && !embedded &&
            <React.Fragment>
              <IconButton variant="contained" color="primary" onClick={() => handleOpenSettings()} data-cy="btnSettings"><SettingsIcon /></IconButton>
              <Button id="btnUpload" disabled={(dataSource === "csv" && file === null) || (dataSource === "es" && !datasets)} variant="contained" color="primary" startIcon={<CloudUploadIcon />} onClick={() => { if (dataSource === "csv") { uploadFile() } else processDataset() }}>Process</Button>
            </React.Fragment>}
          {processing && <CircularProgress variant={upload < 0 ? "indeterminate" : "determinate"} value={upload} />}
        </Grid>
        {processing && <Grid item xs={7}>
          <Typography variant={"body2}"}>{log}</Typography>
        </Grid>}

        {!processing && data !== null && !embedded && <Grid item xs={7}>
          <Button variant="contained" color="primary" style={{ float: "right" }} download={data.jobID + ".csv"} href={`./jobs/${data.jobID}/result`} startIcon={<CloudDownloadIcon />}>Download CSV Results</Button>
        </Grid>}
      </Grid>

      {data !== null && <ResultDisplay anonymize={anonymize} data={data} graph={graph} jobID={jobID} />}
      {data === null && !embedded && <Info />}

      {!embedded && <Dialog
        open={openSettings}
        onClose={handleCloseSettings}
        maxWidth="md">
        <DialogContent>
          <Settings settings={settings} setSettings={setSettings} />
        </DialogContent>
      </Dialog>}
    </div>
  );
}

export default App;
