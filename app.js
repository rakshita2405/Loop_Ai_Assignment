const express=require("express");
const app=express();
app.use(express.json());

let iCount=0;
let bCount=0;

const ingestion={};
const batch={};
const bQ=[];



app.post('/ingest',(req,res)=>{
  const {ids,priority}=req.body;
  if (!Array.isArray(ids) || !priority) {
    return res.status(400).json({error: 'invalid'});
  }

  const ingestion_id='ingest_'+(iCount++);
  const time=new Date();
  const batchIds=[];

  for (let i=0;i<ids.length;i+=3){
    const batch_id='batch_'+(bCount++);
    const bSlice=ids.slice(i,i + 3);
    batch[batch_id] = {
      batch_id,
      ingestion_id,
      ids: bSlice,
      status: 'yet_to_start'
    };
    bQ.push({
      batch_id,
      priority,
      time
    });
    batchIds.push(batch_id);
  }


  ingestion[ingestion_id] = {
    ingestion_id,
    priority,
    time,
    batches: batchIds
  };
  res.json({ingestion_id});
});

function getPValue(priority) {
  if(priority==="HIGH")return 1;
  if(priority==="MEDIUM")return 2;
  return 3; 
}

function NextBatch() {
  if(bQ.length===0)return;

  bQ.sort((a,b) => {
    const pa=getPValue(a.priority);
    const pb=getPValue(b.priority);
    if(pa!==pb)return pa - pb;
    return new Date(a.time)-new Date(b.time);
  });

  const next=bQ.shift();
  if (!next)return;

  const batchObj=batch[next.batch_id];
  if(!batchObj) return;

  batchObj.status="triggered";

 
  setTimeout(() => {
    batchObj.status = "completed";
  }, 1000 * batchObj.ids.length);
}


setInterval(NextBatch, 5000);

app.get('/status/:ingestion_id', (req, res) => {
  const { ingestion_id } = req.params;
  const ing = ingestion[ingestion_id];
  if (!ing) return res.status(404).json({ error: "not found" });

  const batchStatuses = ing.batches.map(batch_id => {
    const b = batch[batch_id];
    return {
      batch_id: b.batch_id,
      ids: b.ids,
      status: b.status
    };
  });

  // Determine overall status
  let overallStatus = "yet_to_start";
  if (batchStatuses.every(b => b.status === "completed")) {
    overallStatus = "completed";
  } else if (batchStatuses.some(b => b.status === "triggered" || b.status === "completed")) {
    overallStatus = "triggered";
  }

  res.json({
    ingestion_id,
    status: overallStatus,
    batches: batchStatuses
  });
});

const PORT=process.env.PORT || 5000;
app.listen(PORT,()=>{
    console.log("Server is running on the port 5000");
})