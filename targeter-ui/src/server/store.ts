import { GetObjectCommand, ListObjectsV2Command, S3Client } from '@aws-sdk/client-s3';
import type { Readable } from 'node:stream';

export interface ArchiveStore { listManifests():Promise<string[]>; get(key:string,max:number):Promise<Uint8Array> }
export class S3ArchiveStore implements ArchiveStore {
  constructor(private client:S3Client,private bucket:string,private prefix:string,private owner:string){}
  async listManifests(){ const found:string[]=[]; let token:string|undefined; do { const r=await this.client.send(new ListObjectsV2Command({Bucket:this.bucket,Prefix:`${this.prefix.replace(/\/$/,'')}/`,ExpectedBucketOwner:this.owner,ContinuationToken:token})); for(const x of r.Contents??[]) if(x.Key?.endsWith('/run_manifest.json')) found.push(x.Key); token=r.IsTruncated?r.NextContinuationToken:undefined; }while(token); return found; }
  async get(key:string,max:number){ const r=await this.client.send(new GetObjectCommand({Bucket:this.bucket,Key:key,ExpectedBucketOwner:this.owner})); if((r.ContentLength??0)>max) throw new Error(`${key} exceeds size limit`); const chunks:Buffer[]=[]; let total=0; for await(const c of r.Body as Readable){ total+=c.length; if(total>max) throw new Error(`${key} exceeds size limit`); chunks.push(Buffer.from(c)); } return Buffer.concat(chunks); }
}
