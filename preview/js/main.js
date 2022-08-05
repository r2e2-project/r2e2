class JobInfo {
  constructor(job_id, bucket, region) {
    this.job_id = job_id;
    this.bucket = bucket;
    this.region = region;
  }

  tile_version_url(tile_id) {
    return `https://${this.bucket}.s3.${this.region}.amazonaws.com/jobs/${this.job_id}/out/${tile_id}`;
  }

  tile_url(tile_id, version) {
    return `https://${this.bucket}.s3.${this.region}.amazonaws.com/jobs/${this.job_id}/out/${tile_id}-${version}.png`;
  }

  status_version_url() {
    return `https://${this.bucket}.s3.${this.region}.amazonaws.com/jobs/${this.job_id}/out/status`
  }

  status_url(version) {
    return `https://${this.bucket}.s3.${this.region}.amazonaws.com/jobs/${this.job_id}/out/status-${version}.json`;
  }
}

class TileHelper {
  constructor(width, height, tile_count) {
    this.width = width;
    this.height = height;
    this.tile_count = tile_count;

    let tile_size = Math.ceil(Math.sqrt(width * height / tile_count));

    while (Math.ceil(1.0 * width / tile_size)
      * Math.ceil(1.0 * height / tile_size)
      > tile_count) {
      tile_size++;
    }

    this.tile_size = tile_size;
    this.n_tiles = {
      x: Math.ceil(1.0 * this.width / this.tile_size),
      y: Math.ceil(1.0 * this.height / this.tile_size)
    };
  }

  bounds(tile_id) {
    const tile_x = tile_id % this.n_tiles.x;
    const tile_y = Math.floor(tile_id / this.n_tiles.x);

    const x0 = tile_x * this.tile_size;
    const x1 = Math.min(x0 + this.tile_size, this.width);
    const y0 = tile_y * this.tile_size;
    const y1 = Math.min(y0 + this.tile_size, this.height);

    return {
      x: x0, y: y0,
      w: x1 - x0, h: y1 - y0
    };
  }
}

const url_params = new URLSearchParams(window.location.search);

const _replay = (url_params.get('replay') === "1");
const _profiling_run = (url_params.get('profiling') === "1");

const _job = new JobInfo(url_params.get('job_id'),
  url_params.get('bucket'),
  url_params.get('region'));

const _tiles = new TileHelper(parseInt(url_params.get('width')),
  parseInt(url_params.get('height')),
  parseInt(url_params.get('tiles')));

let ctx = document.getElementById("output").getContext('2d');

let sidebar = document.getElementById("sidebar");
let canvas = document.getElementById("output");
canvas.width = _tiles.width;
canvas.height = _tiles.height;

if (_profiling_run) {
  canvas.width = 1280;
  canvas.height = 720;
}

// should we resize?
const MARGIN = 200;
let ideal_width = _tiles.width + MARGIN + sidebar.offsetWidth;
let ideal_height = _tiles.height + MARGIN;
let actual_width = window.innerWidth;
let actual_height = window.innerHeight;
var scale_factor = 1.0;

if (actual_width < ideal_width) {
  scale_factor = (actual_width - MARGIN - sidebar.offsetWidth) / _tiles.width;
  canvas.width = actual_width - MARGIN - sidebar.offsetWidth;
  canvas.height *= scale_factor;
} else if (actual_height < ideal_height) {
  scale_factor = (actual_height - MARGIN) / _tiles.height;
  canvas.height = actual_height - MARGIN;
  canvas.width *= scale_factor;
}

let _tile_versions = new Array(_tiles.n_tiles.x * _tiles.n_tiles.y).fill(-1);

function nFormatter(num, digits) {
  const lookup = [
    { value: 1, symbol: " " },
    { value: 1e3, symbol: " k" },
    { value: 1e6, symbol: " M" },
    { value: 1e9, symbol: " G" },
    { value: 1e12, symbol: " T" },
    { value: 1e15, symbol: " P" },
    { value: 1e18, symbol: " E" }
  ];
  const rx = /\.0+$|(\.[0-9]*[1-9])0+$/;
  var item = lookup.slice().reverse().find(function (item) {
    return num >= item.value;
  });
  return item ? (num / item.value).toFixed(digits).replace(rx, "$1") + item.symbol : "0 ";
}

let load_image = (url, ox, oy, ow, oh) => {
  let img = new Image();
  img.crossOrigin = "anonymous";

  let x = ox * scale_factor;
  let y = oy * scale_factor;
  let w = ow * scale_factor;
  let h = oh * scale_factor;

  img.onload = () => {
    W = Math.min(w / 4, 15);
    H = Math.min(h / 4, 15);
    p = Math.min(w / 4, h / 4, 4);

    x0 = x + p;
    y0 = y + p;
    x1 = x + w - p;
    y1 = y + h - p;

    ctx.strokeStyle = 'rgba(255, 255, 0, 0.5)';
    ctx.fillStyle = 'rgba(255, 255, 0, 0.05)';
    ctx.lineWidth = 1.5;

    //ctx.fillRect(x0, y0, x1 - x0, y1 - y0);
    ctx.fillRect(x, y, w, h);

    ctx.beginPath();
    ctx.moveTo(x0, y0 + H);
    ctx.lineTo(x0, y0);
    ctx.lineTo(x0 + W, y0);
    ctx.stroke();

    ctx.beginPath();
    ctx.moveTo(x1, y0 + H);
    ctx.lineTo(x1, y0);
    ctx.lineTo(x1 - W, y0);
    ctx.stroke();

    ctx.beginPath();
    ctx.moveTo(x1, y1 - H);
    ctx.lineTo(x1, y1);
    ctx.lineTo(x1 - W, y1);
    ctx.stroke();

    ctx.beginPath();
    ctx.moveTo(x0, y1 - H);
    ctx.lineTo(x0, y1);
    ctx.lineTo(x0 + W, y1);
    ctx.stroke();

    setTimeout(() => {
      ctx.drawImage(img, x, y, w, h);
    }, 250);
  };

  img.src = url;
};

function randint(min, max) {
  min = Math.ceil(min);
  max = Math.floor(max);
  return Math.floor(Math.random() * (max - min) + min);
}

let get_version = (tile_id, url) => {
  let xhr = new XMLHttpRequest();

  xhr.onreadystatechange = () => {
    if (xhr.readyState == XMLHttpRequest.DONE) {
      if (xhr.status == 200) {
        const new_ver = parseInt(xhr.responseText);
        if (new_ver > _tile_versions[tile_id]) {
          _tile_versions[tile_id] = _replay ? (_tile_versions[tile_id] + 1) : new_ver;
          const bounds = _tiles.bounds(tile_id);
          const tile_url = _job.tile_url(tile_id, _tile_versions[tile_id]);
          load_image(tile_url, bounds.x, bounds.y, bounds.w, bounds.h);
        }
      }

      setTimeout(() => get_version(tile_id, url), 2000 + randint(-750, 750));
    }
  };

  xhr.open('GET', url + "?d=" + new Date().getTime());
  xhr.send(null);
}

if (!_profiling_run) {
  for (let i = 0; i < _tiles.n_tiles.x * _tiles.n_tiles.y; i++) {
    const bounds = _tiles.bounds(i);
    const tile_url = _job.tile_url(i);
    const tile_ver_url = _job.tile_version_url(i);

    get_version(i, tile_ver_url);
  }
}

let save_btn = document.getElementById("save-button");
save_btn.onclick = () => {
  var link = document.createElement('a');
  link.download = `output-${_job.job_id}.png`;
  link.href = document.getElementById("output").toDataURL("image/png");
  link.click();
};

let replay_btn = document.getElementById("replay-button");

if (_replay) {
  replay_btn.classList.add("active");
  replay_btn.title = "Show the latest available output";
}
else {
  replay_btn.classList.remove("active");
}

replay_btn.onclick = () => {
  var url_params = new URLSearchParams(window.location.search);
  url_params.set("replay", _replay ? "0" : "1");
  window.location.search = url_params.toString();
};


// const bandwidthChartCtx = document.getElementById("bandwidth-chart");
// const bandwidthChart = new uPlot({
//   id: "bandwidth-uplot",
//   width:250,
//   height:100,
//   series: [
//     {},
//     {
//       show:true,
//       stroke: "red",
//     }
//   ],
//   axes: [
//     {},
//     {
//       grid: {show: false}
//     }
//   ],
//   scales: {
//     x: { time: false }
//   }
// }, [],
// bandwidthChartCtx);

// var data = [ [0], [0] ];
// var i = 0;

// setInterval(()=> {
//   i += 1;
//   data[0].push(i);
//   data[1].push(Math.floor(Math.random() * 50));
//   bandwidthChart.setData(data);
// }, 1000);

const bandwidthChartCtx = document.getElementById("bandwidth-chart");
const bandwidthChart = new Chart(bandwidthChartCtx, {
  type: 'line',
  options: {
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 0 },
    color: 'rgb(0,191,255)',
    borderColor: 'rgb(0,191,255)',
    plugins: {
      legend: { display: false },
      tooltip: { enabled: false }
    },
    scales: {
      grid: { display: false },
      x: { ticks: { color: 'rgb(0,191,255)' }, display: false },
      y: {
        position: 'right',
        ticks: {
          font: { family: 'Work Sans', weight: 600 },
          color: 'rgb(0,191,255)',
          callback: (label, index, labels) => nFormatter(8 * label, 1) + "bps"
        }
      }
    },
  },
  data: {
    labels: [0],
    datasets: [{
      data: [],
      fill: false,
      tension: 0.15,
      pointRadius: 0
    }]
  }
});

const finishedChartCtx = document.getElementById("finished-chart");
const finishedChart = new Chart(finishedChartCtx, {
  type: 'line',
  options: {
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 0 },
    color: 'rgb(255,56,0)',
    borderColor: 'rgb(255,56,0)',
    plugins: {
      legend: { display: false },
      tooltip: { enabled: false }
    },
    scales: {
      grid: { display: false },
      x: { ticks: { color: 'rgb(255,56,0)' }, display: false },
      y: {
        position: 'right',
        ticks: {
          font: { family: 'Work Sans', weight: 600 },
          color: 'rgb(255,56,0)',
          callback: (label, index, labels) => nFormatter(label, 1)
        }
      }
    },
  },
  data: {
    labels: [0],
    datasets: [{
      data: [0],
      fill: false,
      tension: 0.15,
      pointRadius: 0
    }]
  }
});

let treeletChart;

if (_profiling_run) {
  let treeletChartCtx = document.getElementById("output");
  treeletChartCtx.style.borderWidth = 0;
  treeletChart = new Chart(treeletChartCtx, {
    type: 'bar',
    options: {
      responsive: false,
      maintainAspectRatio: true,
      //animation: { duration: 0.2 },
      color: 'rgb(255,255,255)',
      borderColor: 'rgb(255,255,255)',
      plugins: {
        legend: { display: false },
        tooltip: { enabled: false }
      },
      scales: {
        x: {
          grid: { display: false },
          type: 'category',
          title: {
            display: true,
            text: 'Scene Object',
            color: 'rgb(255,255,255)',
            font: { family: 'Work Sans', weight: 600, size: 22 },
            padding: { top: 15 }
          },
          ticks: { color: 'rgb(255,255,255)' }, display: true
        },
        y: {
          grid: { 
            display: true,
            drawTicks: true,
            drawBorder: false,
            drawOnChartArea: true,
            color: 'rgba(255, 255, 255, 0.2)' },
          type: 'logarithmic',
          title: {
            display: true,
            text: 'Rays Processed',
            color: 'rgb(255,255,255)',
            font: { family: 'Work Sans', weight: 600, size: 22 },
            padding: { bottom:15 }
          },
          beginAtZero: true,
          position: 'left',
          ticks: {
            font: { family: 'Work Sans', size: 18 },
            color: 'rgb(255,255,255)',
            callback: (tickValue, index, ticks) => {
              if (tickValue === 0) {
                return '0';
              }
              const remain = tickValue / (Math.pow(10, Math.floor(Math.log10(tickValue))));
              if (remain === 1 || remain === 2 || remain === 5) {
                return nFormatter(tickValue, 0);
              }
              return '';
            }
          }
        }
      },
      layout: {
        padding: {right: 40}
      }
    },
    data: {
      labels: [],
      datasets: [{
        label: 'test',
        data: [],
        backgroundColor: 'rgba(255, 255, 255, 0.7)',
        barPercentage: 0.99,
        categoryPercentage: 1,
        borderWidth: 0
      }]
    }
  });
}

let _status_version = -1;

let _status = {
  'progress': document.getElementById("data-progress"),
  'workers': document.getElementById("data-workers"),
  'memory': document.getElementById("data-memory"),
  'downloaded': document.getElementById("data-downloaded"),
  'uploaded': document.getElementById("data-uploaded"),
  'elapsed_time': document.getElementById("data-elapsed-time"),
  'throughput': document.getElementById("data-throughput"),
  'peak_throughput': document.getElementById("data-peak-throughput"),
  'traced': document.getElementById("data-traced"),
  'peak_traced': document.getElementById("data-peak-traced"),
};

let load_status = (url) => {
  let xhr = new XMLHttpRequest();

  xhr.onreadystatechange = () => {
    if (xhr.readyState == XMLHttpRequest.DONE) {
      if (xhr.status == 200) {
        json_data = JSON.parse(xhr.responseText);
        const time = parseInt(json_data['timeElapsed']);

        _status.elapsed_time.innerHTML =
          `${String(Math.floor(time / 60))
            .padStart(2, '0')}:${String(time % 60).padStart(2, '0')}`;

        _status.progress.innerHTML =
          parseFloat(json_data['completion'])
            .toFixed(1)
            .replace(/[.,]0$/, "");

        _status.workers.innerHTML = `${3 * json_data['workers']}`;
        _status.memory.innerHTML = nFormatter(4096 * 1024 * 1024 * json_data['workers'], 1);
        _status.downloaded.innerHTML =
          nFormatter(parseInt(json_data['bytesDownloaded'])
            + parseInt(json_data['bytesUploaded']), 1);

        bandwidthChart.data.labels = json_data['throughputsX'].map((x) => parseInt(x));
        bandwidthChart.data.datasets[0].data = json_data['throughputsY'].map((x) => parseInt(x));

        while (bandwidthChart.data.labels.length > 0 &&
          bandwidthChart.data.labels.length < 30) {
          bandwidthChart.data.labels.push(
            bandwidthChart.data.labels[bandwidthChart.data.labels.length - 1] + 1);
          bandwidthChart.data.datasets[0].data.push(null);
        }

        bandwidthChart.update();

        _status.throughput.innerHTML =
          nFormatter(8 * json_data['throughputsY'][json_data['throughputsY'].length - 1], 1);
        _status.peak_throughput.innerHTML =
          nFormatter(8 * json_data['throughputsPeak'], 1);

        _status.traced.innerHTML =
          nFormatter(json_data['pathsFinishedY'][json_data['pathsFinishedY'].length - 1], 0);
        _status.peak_traced.innerHTML =
          nFormatter(json_data['pathsFinishedPeak'], 0)

        finishedChart.data.labels = json_data['pathsFinishedX'].map((x) => parseInt(x));
        finishedChart.data.datasets[0].data = json_data['pathsFinishedY'].map((x) => parseInt(x));

        while (finishedChart.data.labels.length > 0 &&
          finishedChart.data.labels.length < 30) {
          finishedChart.data.labels.push(
            finishedChart.data.labels[finishedChart.data.labels.length - 1] + 1);
          finishedChart.data.datasets[0].data.push(null);
        }

        finishedChart.update();

        if (_profiling_run) {
          if (treeletChart.data.labels.length == 0) {
            for (let i = 0; i < json_data['raysTracedPerTreelet'].length; i++) {
              treeletChart.data.labels.push(i);
            }
          }

          if (treeletChart.data.datasets[0].data.length == 0) {
            for (let i = 0; i < json_data['raysTracedPerTreelet'].length; i++) {
              treeletChart.data.datasets[0].data.push(i);
            }
          }

          //const total = 1.0 * json_data['raysTracedPerTreelet'].reduce((partialSum, a) => partialSum + parseInt(a), 0);
          for (let i = 0; i < json_data['raysTracedPerTreelet'].length; i++) {
            treeletChart.data.datasets[0].data[i] = json_data['raysTracedPerTreelet'][i];
          }

          treeletChart.update();
        }
      }
    }
  };

  xhr.open('GET', url);
  xhr.send(null);
};


let get_status_version = (url) => {
  let xhr = new XMLHttpRequest();

  xhr.onreadystatechange = () => {
    if (xhr.readyState == XMLHttpRequest.DONE) {
      if (xhr.status == 200) {
        const new_ver = parseInt(xhr.responseText);
        if (new_ver > _status_version) {
          _status_version = _replay ? (_status_version + 1) : new_ver;
          const status_url = _job.status_url(_status_version);
          load_status(status_url);
        }
      }

      setTimeout(() => get_status_version(url), 1000);
    }
  };

  xhr.open('GET', url + "?d=" + new Date().getTime());
  xhr.send(null);
}

get_status_version(_job.status_version_url());
