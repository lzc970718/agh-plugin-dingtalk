import {createRequire} from 'node:module'
import {mkdir,readFile,writeFile,readdir} from 'node:fs/promises'
import {dirname,join,resolve} from 'node:path'
import {fileURLToPath} from 'node:url'
const here=dirname(fileURLToPath(import.meta.url)),own=createRequire(import.meta.url)
const {build}=own('esbuild')
await mkdir(join(here,'dist/adapters/dingtalk'),{recursive:true})
const icon=await readFile(join(here,'assets/dingtalk.png'))
const manifest=JSON.parse(await readFile(join(here,'src/adapters/dingtalk/channel.json'),'utf8'))
manifest.icon='data:image/png;base64,'+icon.toString('base64')
if(manifest.icon.length>32768)throw new Error('Channel icon exceeds manifest budget')
const manifestJson=JSON.stringify(manifest,null,2)+'\n'
const result=await build({absWorkingDir:here,entryPoints:['src/index.ts'],outfile:'dist/index.mjs',bundle:true,platform:'node',format:'esm',target:'node24',metafile:true,
  external:['@agnes/plugin-runtime/channel-contract','@agnes/protocol'],
  plugins:[{name:'channel-icon',setup(builder){builder.onLoad({filter:/[\\/]adapters[\\/]dingtalk[\\/]channel\.json$/},()=>({contents:manifestJson,loader:'json'}))}}],
  banner:{js:"import { createRequire as __createRequire } from 'node:module'; const require = __createRequire(import.meta.url);"},
})
await writeFile(join(here,'dist/adapters/dingtalk/channel.json'),manifestJson)
let notices='AGH DingTalk channel plugin\nApache-2.0. Adapted from Agnes Harness channels.\n'
const seen=new Set()
for(const input of Object.keys(result.metafile.inputs)){
  let dir=dirname(resolve(here,input))
  while(dirname(dir)!==dir){
    try{
      const p=JSON.parse(await readFile(join(dir,'package.json'),'utf8'))
      if(!seen.has(p.name)){
        seen.add(p.name);const license=(await readdir(dir)).find(n=>/^(license|copying)(\.|$)/i.test(n))
        notices+=`\n--- ${p.name}@${p.version} (${p.license??'see license'}) ---\n`
        if(license)notices+=await readFile(join(dir,license),'utf8')
      }
      break
    }catch{dir=dirname(dir)}
  }
}
notices+='\nDingTalk brand icon: assets/dingtalk.png, user-provided source URL recorded in PROVENANCE.md. Brand rights belong to their respective owner.\n'
await writeFile(join(here,'NOTICE'),notices)
await writeFile(join(here,'dist/build-info.json'),JSON.stringify({node:process.version,apiRange:'^5.8.0',bridge:'host-owned',bundled:[...seen].sort()},null,2))
console.log('Built DingTalk adapter; runner, SDK, daemon discovery and native libraries are host-owned')
