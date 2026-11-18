import{api,post}from'./client.js';
export const listBatches=()=>api('/batches');
export const replicaHealth=()=>api('/batches/replica-health');
export const createBatch=data=>post('/batches',data);
