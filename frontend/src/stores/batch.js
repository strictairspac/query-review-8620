import{listBatches,replicaHealth}from'../api/batch.js';
export const batches={items:[],health:null,async load(){const [items,health]=await Promise.all([listBatches(),replicaHealth().catch(()=>null)]);this.items=items;this.health=health;return this.items}};
