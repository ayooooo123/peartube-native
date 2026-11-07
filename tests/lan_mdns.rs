//! mDNS parity with the app's `src/lan.js` and live interop with `bonjour-service`.
//!
//! The fixture below is produced by `tests/js/lan-mdns.mjs fixtures`, which holds the app's own
//! `buildTxt`/`createRecords`/`parseRecord` copies, so both the record bytes and the parser
//! verdicts come from the JavaScript implementation.

use std::io::{BufRead, BufReader, Write};
use std::net::{IpAddr, Ipv4Addr, UdpSocket};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};

use peartube_native::lan::mdns::{
    DiscoveredService, ErrorSink, SelfAddressAdapter, ServiceSink, create_records_with,
    parse_record, topic_token,
};
use serde_json::Value;

const FIXTURE: &str = r##"{"cases":[{"peerKey":"be73072c2bc63afeeaf5476265f70c79cbbd8d8340649d9367333001ac968ce3","port":4100,"nonce":"a0z","topics":[],"records":[{"name":"hyperdht-mdns-be73072c2bc6-47DEQpj8a0z-0-1","type":"hyperdht-mdns","protocol":"udp","port":4100,"txt":{"v":"1","peerKey":"be73072c2bc63afeeaf5476265f70c79cbbd8d8340649d9367333001ac968ce3","port":"4100","tc":"0"}}],"parsed":[{"generation":"47DEQpj8a0z","shard":0,"totalShards":1}]},{"peerKey":"9c3dc885d4d5bc0e3d21f5efb9a57e3671863923ecd66c3c29cba6fc98dfd530","port":4321,"nonce":"001","topics":["topic-1-0"],"records":[{"name":"hyperdht-mdns-9c3dc885d4d5-J2hQy4qD001-0-1","type":"hyperdht-mdns","protocol":"udp","port":4321,"txt":{"v":"1","peerKey":"9c3dc885d4d5bc0e3d21f5efb9a57e3671863923ecd66c3c29cba6fc98dfd530","port":"4321","tc":"1","t0":"Vq3NJ5MwaIwQJB17jYvErMUKcZ9BI3mwCv-4K-AE3IE"}}],"parsed":[{"generation":"J2hQy4qD001","shard":0,"totalShards":1}]},{"peerKey":"63c65947c8774a1fbd2565cc0232ec4a720b1f98e5c72069f507f9cdadcaae20","port":5000,"nonce":"zzz","topics":["topic-2-0","topic-2-1"],"records":[{"name":"hyperdht-mdns-63c65947c877-Richtbywzzz-0-1","type":"hyperdht-mdns","protocol":"udp","port":5000,"txt":{"v":"1","peerKey":"63c65947c8774a1fbd2565cc0232ec4a720b1f98e5c72069f507f9cdadcaae20","port":"5000","tc":"2","t0":"FKC-Qpy33NFuDZeaz0tddlDVBYXd4vPWb18WAXIo77Y,SA3nEnM2JHT9rYyah7zhKbTYPEIqpf41-n-oHlAjE2I"}}],"parsed":[{"generation":"Richtbywzzz","shard":0,"totalShards":1}]},{"peerKey":"b39b312d4544064ed303a84de6e17e7c4109b30eb6cbe9f79caeb430ca6ec983","port":65535,"nonce":"q9x","topics":["topic-3-0","topic-3-1","topic-3-2"],"records":[{"name":"hyperdht-mdns-b39b312d4544-YrikPEU5q9x-0-1","type":"hyperdht-mdns","protocol":"udp","port":65535,"txt":{"v":"1","peerKey":"b39b312d4544064ed303a84de6e17e7c4109b30eb6cbe9f79caeb430ca6ec983","port":"65535","tc":"3","t0":"6paf4D-j3qfBU36kZ9KG0VvrmAyC752vJWE2d9_KmCU,MyDs1XmGgWohKoqDt0EbAMBXjuik8LELoJxqLe_jD1w,trlX_9jPM3m0QRk3mR-tna1A6fPItNwdwCpK4gQ5_W8"}}],"parsed":[{"generation":"YrikPEU5q9x","shard":0,"totalShards":1}]},{"peerKey":"8ca5fe0b71ba4c69187e8be496fb48140e20ff1fd26e573bca6cdba18d4865e1","port":8080,"nonce":"k7m","topics":["topic-4-0","topic-4-1","topic-4-2","topic-4-3"],"records":[{"name":"hyperdht-mdns-8ca5fe0b71ba-rrqz5nqqk7m-0-1","type":"hyperdht-mdns","protocol":"udp","port":8080,"txt":{"v":"1","peerKey":"8ca5fe0b71ba4c69187e8be496fb48140e20ff1fd26e573bca6cdba18d4865e1","port":"8080","tc":"4","t0":"8fdCkEX-eAyGME0rAf0yMaSGtKE66CAVHyKVFDcZJE8,NaSfgxBRvFp7xOP33YoU3x8vBk1utUDB5vQuWcGuz5A,ckFQx0MIydr6dS8ui-Qlt0e58XMh9E8EDQqTO2zTFK4,emSbid6dfLOA1ssxD9_hsVlaTvS-780N_l9IrGTcvuY"}}],"parsed":[{"generation":"rrqz5nqqk7m","shard":0,"totalShards":1}]},{"peerKey":"382e932c70ad9773c1b13f9ecc328ea8f17ace233b2b3a7ee76c9168e7d01698","port":4100,"nonce":"a0z","topics":["topic-5-0","topic-5-1","topic-5-2","topic-5-3","topic-5-4"],"records":[{"name":"hyperdht-mdns-382e932c70ad-Mf4aLPEfa0z-0-1","type":"hyperdht-mdns","protocol":"udp","port":4100,"txt":{"v":"1","peerKey":"382e932c70ad9773c1b13f9ecc328ea8f17ace233b2b3a7ee76c9168e7d01698","port":"4100","tc":"5","t0":"0E5TraJzbaXY7BLpDqo0m37wh3KLCDEExqQ3iUBnC6E,J6KxDlrbVZEDGUtSMu3uOijfJBnTMk0yNpCB0bdYEyE,Ngv_gJ3MzX_iT7FM-4LQ2c1deH4UkSYh2bu3Mxbu0vw,fABA2E_t6f2bhx2M_oIzXWjMxU5rrgTJMKX2L-KxYUo,jHTA_kEd_N56Ufl2B-9tuXaW09HTODNSvrnx5lLnwxY"}}],"parsed":[{"generation":"Mf4aLPEfa0z","shard":0,"totalShards":1}]},{"peerKey":"a12c8cacae2c9186177ffafa3da6ba9e98ce12580679fc0a4574734e539aaf6d","port":4321,"nonce":"001","topics":["topic-6-0","topic-6-1","topic-6-2","topic-6-3","topic-6-4","topic-6-5"],"records":[{"name":"hyperdht-mdns-a12c8cacae2c-ZnJpqDzp001-0-1","type":"hyperdht-mdns","protocol":"udp","port":4321,"txt":{"v":"1","peerKey":"a12c8cacae2c9186177ffafa3da6ba9e98ce12580679fc0a4574734e539aaf6d","port":"4321","tc":"6","t0":"3s3qAIhipn0uAaWw8ghX3Bze3zYm3p8MoLHL7TxFpF0,Az8gJP4KgHuSoRcWxkpRHfs2jWFeDj6jJGq7nzHCLsM,FmnM5IsURwbWZko__e2b4Guts51Fin9B6OLOgKdPBYs,WUvbyYQZo3hHm_Bn9n2MJESxq9PebqPTFsI9UgmBzTc,XXxebQAz7mgE_Yo62YzWyIQAIMyWSv_B1Kv3enoD-RI","t1":"pOT5zNCsImQ3bPQl_aazInbwm_Jpn9x6r0MXN0gxtVk"}}],"parsed":[{"generation":"ZnJpqDzp001","shard":0,"totalShards":1}]},{"peerKey":"cdbb02060f9e0608cce1b80a1a5a2af76b3b6c03b878b6d9b107c97a2a687e3a","port":5000,"nonce":"zzz","topics":["topic-7-0","topic-7-1","topic-7-2","topic-7-3","topic-7-4","topic-7-5","topic-7-6"],"records":[{"name":"hyperdht-mdns-cdbb02060f9e-YUjOuMYBzzz-0-1","type":"hyperdht-mdns","protocol":"udp","port":5000,"txt":{"v":"1","peerKey":"cdbb02060f9e0608cce1b80a1a5a2af76b3b6c03b878b6d9b107c97a2a687e3a","port":"5000","tc":"7","t0":"0QFpxvQps2uxK3fkcKQpPcYBK8M-EdQxd9_nA4yNqm4,2J1dEXlidUMVq9hETv2_CmYaRuKPcDdA-l0BdT97fpA,3cXMSocYX--HqJ7vOz2Id0fYuMX1JcZrH33IX30nhK8,AA-SkdJL6mf3EmSaNTQLkh2BsNrQ_w6wBwlapFr5FkQ,Lb4v7PeTA4T9nHsCl0fS5sk4WAAXkcUcG6r1Uer7KJo","t1":"_HZB1ieRzkcmLaSkQ8wNDO9XERlJNNDrcsNq-R_KWAA,zetVh286T60-rMMVWuSJxIlYfx-YV5oVDsnoFWYD_ig"}}],"parsed":[{"generation":"YUjOuMYBzzz","shard":0,"totalShards":1}]},{"peerKey":"6a55b6c2e6a33cd713cf76e10fc78af60917787a84b4f3c62c62afa1301608a3","port":65535,"nonce":"q9x","topics":["topic-8-0","topic-8-1","topic-8-2","topic-8-3","topic-8-4","topic-8-5","topic-8-6","topic-8-7"],"records":[{"name":"hyperdht-mdns-6a55b6c2e6a3-wQHLdB-4q9x-0-1","type":"hyperdht-mdns","protocol":"udp","port":65535,"txt":{"v":"1","peerKey":"6a55b6c2e6a33cd713cf76e10fc78af60917787a84b4f3c62c62afa1301608a3","port":"65535","tc":"8","t0":"09ayyXHMdVk_om470poTEdcUferp8EmYvNrj1a5owA8,20XTsG4F2IJfuH6XqQq-9UpUmmtr_XnSVr_0BWi6ZSo,84VDaPvfESI-FaWZ9cBl1qVWNYALk9IxQFaPhudsir0,Bx6Jq_uxQzguS5vqcLfMumczVTGUeb0zL2WegGFLd5g,H60CW00fZNI9KIHQ0NuVebU_BGbXCO4q9_2hNxm6vPI","t1":"I5gcQG2aCsIXUP6ZM6tI1Jfl2UmwsbirpGm6P1rmLj0,j_NyGcjbzpq9Mjuj7jide-4wfotHftnzwNI91gUDdcc,rO94gX4qZg2MSb1TaS1UsxX0a8FOpi9i7n5Ca6ofHHw"}}],"parsed":[{"generation":"wQHLdB-4q9x","shard":0,"totalShards":1}]},{"peerKey":"4f30d19a64c9e0e7ed9fe4648917ad6936b10a6f3e8d490e7f003dffd49aee29","port":8080,"nonce":"k7m","topics":["topic-9-0","topic-9-1","topic-9-2","topic-9-3","topic-9-4","topic-9-5","topic-9-6","topic-9-7","topic-9-8"],"records":[{"name":"hyperdht-mdns-4f30d19a64c9-cSGmiFKqk7m-0-1","type":"hyperdht-mdns","protocol":"udp","port":8080,"txt":{"v":"1","peerKey":"4f30d19a64c9e0e7ed9fe4648917ad6936b10a6f3e8d490e7f003dffd49aee29","port":"8080","tc":"9","t0":"5dBwAKzdQV592lBwuoDF94bVma7UVg57QTx__xhzvUY,GjFhWMvgJV3WmxYPf58wlI3SEIA_4_uorn2TTrNs1R8,avVelNj5IV4cX3OejvfnQ7FNEU2iNX7N7_tnDkat7_k,dO8xler3akBOSNgI9XAd5bXg4WMqHa0PWNV4bv66-ng,iY6X91HLBrmEwqEx3L64U2isLsbt-lqiqr8evy2PuRg","t1":"pudAP1Udj09-BN-u3EFJM6ue3rTrPZvJBMqp4O0VOp8,rI1oNiYehzjeuXAiYjzM7oBReqMH3MHZfhlKL0Cwylk,tiV4yeHiqvHHOHRlsdYXOdFH5DeOfzFijZZpOLAuhHg,yp5HtCAMUlEa_Xi3owrVF3FpPGT-fUDFNMAt36eJDVw"}}],"parsed":[{"generation":"cSGmiFKqk7m","shard":0,"totalShards":1}]},{"peerKey":"4d7ca3c3d56cf29c600415ffb2b98680a968863e08c1e0b8e5c9c515e102d4ec","port":4100,"nonce":"a0z","topics":["topic-10-0","topic-10-1","topic-10-2","topic-10-3","topic-10-4","topic-10-5","topic-10-6","topic-10-7","topic-10-8","topic-10-9"],"records":[{"name":"hyperdht-mdns-4d7ca3c3d56c-o_jS1f6ua0z-0-1","type":"hyperdht-mdns","protocol":"udp","port":4100,"txt":{"v":"1","peerKey":"4d7ca3c3d56cf29c600415ffb2b98680a968863e08c1e0b8e5c9c515e102d4ec","port":"4100","tc":"10","t0":"Dgls6iDM1GwaIGkG0phVgdVp5rY5WMzPWK4IgqpYstc,GUDFYwwGP7qVJ2ojd-T3HYknkg7-lF2WrugfHsXyTHk,O_yTE1YcNvSiHKaO_Yv9QYumbM4_iD55HFyx0hAyGnQ,YIoFyJ4mkmb-f-4jbGWSKvyykoucdpC89pavugfZm6c,YXsnqeZRpy9QiFYdeKeByYuo6DvcLJdTGgqnEl1qn9k","t1":"d1utoRlZRjia6QVV6U9DvBdy6PI_Xt2zf6EOgADAdio,gDXn2XWMA_885z5V-2WJmyUSf_YLdqGEm4OiDjCzwA8,hX-mTgl_MwzwzdhntjSPbsAXdQ5bwTVLdMl1vMph3wo,n8hcBo_IlO4XXFW-ROGlt8zyWfl1OLgT4-XKWYY3TZo,ncD1GkPP3db0EcoLoKbagl_rk7OxRaXOxND6vrFLwi8"}}],"parsed":[{"generation":"o_jS1f6ua0z","shard":0,"totalShards":1}]},{"peerKey":"095ae41215f3cdc0ec54fe8487f2f19ee200c8a707b65eeb680bbac85e1368d2","port":4321,"nonce":"001","topics":["topic-11-0","topic-11-1","topic-11-2","topic-11-3","topic-11-4","topic-11-5","topic-11-6","topic-11-7","topic-11-8","topic-11-9","topic-11-10"],"records":[{"name":"hyperdht-mdns-095ae41215f3-IHavqr4G001-0-1","type":"hyperdht-mdns","protocol":"udp","port":4321,"txt":{"v":"1","peerKey":"095ae41215f3cdc0ec54fe8487f2f19ee200c8a707b65eeb680bbac85e1368d2","port":"4321","tc":"11","t0":"2Vgz7pQ-LQPzmKcz3v7dISg9XUcbCvOM1aRDDfcd9LE,IjYZsRfET7qxKxbFLsJwUJLcFM0SQc2ZMca2W0HnoVU,MEqwzanVs2JUlppmFbzG9lhOMQ60r-ie3uf0gwOiWfY,MR-nFLQIddhzapTAGGE2JvANsRw2Zi8lw0ykYOzqUUc,MqdsqVJ9_e44HdXyj8qljRLAmuEZcCGVaCeE_EO6o-w","t1":"PY4PCFCw4AcxKhNAMFKeTV-EiZNjGEuP0ufiNbANQlg,ZkKf_aJwTOVNOpJmI5V8flx8zbno8AS-4Tf0f-xKvnU,_nOV5Uu8FqbMvqiM_qJWtqyDgklr14v2zHv2iqm_ct8,b_DuD1AIxkXhEc6dFmXwIAurMthMjpfOe0aB2v-iB9A,c8PtB6jlR1LY8Y5Ys3mWJ0reKE7DC0LlRtwhfVrLhOE","t2":"h4wUQxEL0uihQiR4ub1EOLTVlWE5H79TE4j4pwLUtEQ"}}],"parsed":[{"generation":"IHavqr4G001","shard":0,"totalShards":1}]},{"peerKey":"631d2b51ed78be9e6a56b69461cda53ed39d090606d007b026ed9941bc6208ba","port":5000,"nonce":"zzz","topics":["topic-12-0","topic-12-1","topic-12-2","topic-12-3","topic-12-4","topic-12-5","topic-12-6","topic-12-7","topic-12-8","topic-12-9","topic-12-10","topic-12-11"],"records":[{"name":"hyperdht-mdns-631d2b51ed78-nQVXyFl6zzz-0-1","type":"hyperdht-mdns","protocol":"udp","port":5000,"txt":{"v":"1","peerKey":"631d2b51ed78be9e6a56b69461cda53ed39d090606d007b026ed9941bc6208ba","port":"5000","tc":"12","t0":"-kv0FOM497-U-NIXzyC95q8hOKkf6q41GR87o5PgMUQ,CY85_kwTUgLuGLMSHx__CrsJ2D-Kq7jEo_p8oAbEm-0,DOqLWW3NY9cMe8cMKo6p2phc7wpWBAzPEJZB1BbFjVw,IK31haM7YiT9cGBch4v3skhXUjeC8gffWS0vi0zvJdA,Ib2JA_8gD_31g6LetfhP39_OlYcLB3RGsho_z8m6p2M","t1":"Kf0gyt3nwhnaEyfip5BfmP0s3wWMPgu0MGxWdbPGtWs,MaWXQcT6b2nL0dmMPwIZfcyTL4swba9i4NMSEg_-6WM,QfgA78IjVt8WVripNarv7HSo4wB0RcTroeyayiaqKz0,S10RFCA9Hi_gKVmyqToRCvY7IP6Gs83MjUk3nEQdC4Y,ZI3GdhP-5dJJDz-l3wdEyHYxBHj5KqfUsGwDrnQP4X8","t2":"lfIftLi-9tRc1vZyBGnEQb42_UG2ZV002bzoqIIf_QA,vtkGJdHFr-3qe3GOHljRzmUmGNs3AwkBSFi_fN1kHnk"}}],"parsed":[{"generation":"nQVXyFl6zzz","shard":0,"totalShards":1}]},{"peerKey":"b94aac7fa80839776806962a2e95cfbf365e13459931acd1636ce19389c11e55","port":65535,"nonce":"q9x","topics":["topic-13-0","topic-13-1","topic-13-2","topic-13-3","topic-13-4","topic-13-5","topic-13-6","topic-13-7","topic-13-8","topic-13-9","topic-13-10","topic-13-11","topic-13-12"],"records":[{"name":"hyperdht-mdns-b94aac7fa808-Xs3MSEvvq9x-0-1","type":"hyperdht-mdns","protocol":"udp","port":65535,"txt":{"v":"1","peerKey":"b94aac7fa80839776806962a2e95cfbf365e13459931acd1636ce19389c11e55","port":"65535","tc":"13","t0":"0yruBpugOPoOpQcQ6w9xXnmGH52u6PuwOltXyB95XKI,20HbCgUHN96qPjOtmNNxso00QIOPNjeF6C9xl-2Y9cI,5VgCYZFvgurzWQZZVrMh2bAqgk-i_pTiI9LqNZUv6bs,7CkIDPRWQewMnACkFMxBs9xjFR0k3bX-Nyx4D8GxFf0,7v9lQQgHMEJsJ_Us-CtmAtlcReiuCBpAzpZURfDzp1g","t1":"7wQfbqQtFxA29JhdjdlbKnubnafD-q-wJant7KzUGmI,F1qn_SFYgML-eR8Qr4JhUY9WKXhbclaKkXFCs9-qyeI,JpUk7uvtt7d3ztCWPIRDYCGyJ-dyK-5E3PcU1CNr7kI,WqRS4-meA1sKu7ZEIGvqQLh3y9UGe0hzU0LWdHVbDh4,gLIxMLld_a7GjfhKPxxfd07htNSExd6CmKElSLks8ok","t2":"q2Yuzmamtr76m1FfQcFi-DJ_bL1pUP3dtqTqZGVgYjo,qrsDtIGixhgwEmfiWY-e-ObsC5brDcrLB-CwQ7dCECs,rSfSbhHez8QwaTL1YkFP6eI0g5rO4nj8953xvytyw6w"}}],"parsed":[{"generation":"Xs3MSEvvq9x","shard":0,"totalShards":1}]},{"peerKey":"3f58ab18a88db43a5940b3e43c6eb29a30fc55fb33702bb58c2260152ae86d83","port":8080,"nonce":"k7m","topics":["topic-14-0","topic-14-1","topic-14-2","topic-14-3","topic-14-4","topic-14-5","topic-14-6","topic-14-7","topic-14-8","topic-14-9","topic-14-10","topic-14-11","topic-14-12","topic-14-13"],"records":[{"name":"hyperdht-mdns-3f58ab18a88d-brOQp0F2k7m-0-1","type":"hyperdht-mdns","protocol":"udp","port":8080,"txt":{"v":"1","peerKey":"3f58ab18a88db43a5940b3e43c6eb29a30fc55fb33702bb58c2260152ae86d83","port":"8080","tc":"14","t0":"9yPbDq_mNgCoEnAKe-sO4rb70euBeLuFOKbRDBj4M_Q,E0UfmkDMdZj8OYeQGIeq3-HPClha6DMubA87Ivj71Kw,HhcXj2irIDU7Bgg_RAVav8pu01sgdjZnHbabQFdxmSA,RwARmlTMe_I_tlwLs_-_XKx57gvmJtHrbDIsVJsBEog,TS6n8z4VDD_rHw--DJVjQZ38LlloHqdv-Ess68rNGj0","t1":"dv0UMmw0RqIevIsjWhlyfVz56HLMi65obITGISHMCqA,eHNa6CbZRBA3VqcF44O0a8A_hm2VG6T19FlNZQXtAbg,ezt0Sd1vh5qUal0amTXhz0uTagVHk__7zhaO_KPvLjk,imq8iKkTxUzYWqbtABrYeEljjHHG9GGn1-4ADYev428,lreKj4km1sl6p_btXjg-gedwtlwvcjYZwvq1UE8xiQM","t2":"typFPCcqgI3LxD6h4-whKaAosWodT3_aLb0P12g8Yr8,vihfaerioDNanWFy9I-TaIewwQvQXUpu35RncBolXqI,w0HuY4QwTADMHAQJ_thZ5qaUvDuyzyhKClrYSNsTkuw,xnECU8JxIo59nU6WBz1bMCnHz4sM0_ak4h3izwTgvnw"}}],"parsed":[{"generation":"brOQp0F2k7m","shard":0,"totalShards":1}]},{"peerKey":"ea500068c780cead69d26a9d6b5d1f79e639cdb180f416fb285da251e83fc3d5","port":4100,"nonce":"a0z","topics":["topic-15-0","topic-15-1","topic-15-2","topic-15-3","topic-15-4","topic-15-5","topic-15-6","topic-15-7","topic-15-8","topic-15-9","topic-15-10","topic-15-11","topic-15-12","topic-15-13","topic-15-14"],"records":[{"name":"hyperdht-mdns-ea500068c780-g5WszeoJa0z-0-1","type":"hyperdht-mdns","protocol":"udp","port":4100,"txt":{"v":"1","peerKey":"ea500068c780cead69d26a9d6b5d1f79e639cdb180f416fb285da251e83fc3d5","port":"4100","tc":"15","t0":"-ZJKK1EOWW9y3EggdnlB3-Jcs7suxoSpi-VXtT551AA,EsP1rmKYefDjpOI6pJFNiosUPArVXSSWksZ40SRAFBc,FIaTg95ddbwn2XuEt-G6T-MUVR8Qjho6x0Kfs8L59Wg,Htl557INLHo2X8DqYLUtW9iYR05uyzuHrKofgZkXHQE,JPzzUwLvsLPc05DS68d1jsny5G4-CbO3-7S9ygKYLOU","t1":"MVUOzuSuhKQMEXrgDrh9onCTkH1uPgvIf8nUn88xWlY,P2zX18wcizHjY_mKtiXyDDatk7S6n1NTR0Li90YtNhg,_AxmRTKIb2Fc638gsisKlQe7bJEyCz7VhgzqRI9sz5Q,coG6YUxbwsgEd-snPtolwqgkA3-Ct6RlSrFIIoCjpLg,fDA725C4I1F_GQIXjxiEzkxRix7FH3Xme2JCIxtXB08","t2":"ll8TTVQQ8-QAvCO66pkFeeBgvN2Cq3x3SbYNkeMAlsQ,nYidjOtP9cMsJ8FbUEIM5OGtY_vjUpXtDKXQ7QGFTa4,qGpyRVGFQNsCJPKfO0Tog3qePSJtDmC9r-O8IcT0UYM,qSh4IuEmhqMsnqTX8iteUwJwnijrInVJKZAsskzk11I,seD9b2vZS2vMRPot60JhtaCzKqM9ZXGgH8m4X0iK9F4"}}],"parsed":[{"generation":"g5WszeoJa0z","shard":0,"totalShards":1}]},{"peerKey":"9bc7ce1ccf9ab0747d3880c32342a9e4ca26b0be1ed0982310730c4080b7ded0","port":4321,"nonce":"001","topics":["topic-16-0","topic-16-1","topic-16-2","topic-16-3","topic-16-4","topic-16-5","topic-16-6","topic-16-7","topic-16-8","topic-16-9","topic-16-10","topic-16-11","topic-16-12","topic-16-13","topic-16-14","topic-16-15"],"records":[{"name":"hyperdht-mdns-9bc7ce1ccf9a-azd_uk7Z001-0-1","type":"hyperdht-mdns","protocol":"udp","port":4321,"txt":{"v":"1","peerKey":"9bc7ce1ccf9ab0747d3880c32342a9e4ca26b0be1ed0982310730c4080b7ded0","port":"4321","tc":"16","t0":"0esbiKxpxTWtze2WDdiMjF1W1_PXepEiMkT3VxbT6Zs,1HQmfTIKAyo3RXz5XlRwL_JQ8lTwhrMv6i5hlLPeP2Y,48Ic-9g3mLPQYv6LWP8NlTpSkh99YYRVT--FTD7PQtQ,AvGDEeVmeTIeaaTAdVtm3fFKzq6ms1CWar30f738CUQ,CMpZfrGhQsdCWc39WBpyaDVIjioHd_CkvL68YbDpIYM","t1":"LoIkp0LrEz-9DLVHL5PsV5Cm0bX0RAdbvWYmUJTm5tE,U6qUFYEpl8Bov4u45-l0AUNyK4VtwoeUT0FOzJY4eoU,UsLNy4fv7S5cYqsEVrJzSbui7rtsyfTyLIBEKDjxQnU,Z_9uEpD4-CX3B7OPwaFp6k09eaF0-ZWRdbBfUW6r7rA,kGZVvEy5iggDXM6VIu8tHP1G-YwonmxUWJpjI5jp19g","t2":"o4B8Y87DVGm4GkfT8ngMfBSNPjtXvOdC9pUMKNEVzqE,p_yiicFtn4sI9OTMt6R5mMBcIcizP1PG8rPGrKOhV6g,tJIRPMajSCcG3zUHRAhSoWPbsOqwHzH9POUwCcY6v2w,tLLixDYNx8osQxduJ0zjRHjkaDzktE6JMJoQhlKelQ4,wnPVtk5WgxSVzX52tmIHHYQSIM6cZyR3XNO4Q-foLfA","t3":"ywVtw6749QoOHTw-3V1YUe1Tv8HF_mTkvFxLsxtx93s"}}],"parsed":[{"generation":"azd_uk7Z001","shard":0,"totalShards":1}]},{"peerKey":"47286e915d055888fc08a8b54338eb3636d59a7a183df65ddae672d08f991bfa","port":5000,"nonce":"zzz","topics":["topic-17-0","topic-17-1","topic-17-2","topic-17-3","topic-17-4","topic-17-5","topic-17-6","topic-17-7","topic-17-8","topic-17-9","topic-17-10","topic-17-11","topic-17-12","topic-17-13","topic-17-14","topic-17-15","topic-17-16"],"records":[{"name":"hyperdht-mdns-47286e915d05-54GjFaM4zzz-0-1","type":"hyperdht-mdns","protocol":"udp","port":5000,"txt":{"v":"1","peerKey":"47286e915d055888fc08a8b54338eb3636d59a7a183df65ddae672d08f991bfa","port":"5000","tc":"17","t0":"-BPxcJOGsg9yiJsHb3Zpbaieb7funpq6iJ833IpPeZw,1LWcKC5eyYU79NL5mX60bh7JVJom7YLztfGXjrM1wsg,NRxoaHgKBdI4kLKbfn9O0-ZJ2RD8NEb2GpOPPyLWzak,OG1J0dQEuv67icbcEio1E0OyuTWa813aqP9Fh3UrGaA,U4PJQnGRmRzzvcD2xMWeGKvD_9U1tzhhMfSUfAD3HVE","t1":"UWz2aktzZnl7oiAx10drNHIBwjyiBxZ5UF9OaWDlYE8,W6i-p4_AgmNxjtXeILpcO3_NlAqQ2Ho2vv4Tokop5nU,eXgmYgaek7VkjtSDpfwHl6IhgP5tHrHwYEbjhIrB6Us,gdE9Z1MY6hGD6WK2xAVghYiit79uNbFRztFrUx8IJqQ,i2CU2GkLAkoCs45X1AEjtgf3oeICPwmG68_x-Uea5zY","t2":"iLW3bj3PCvhOmlA_GgFwtr-pPVEJwTc-rnfQgKyJjJg,mRZDC5Xo6laGM8nxnz6rbuPVG-DVNXnPEeoDATG2v2A,muSzu4G-b6xAlZiM1qPsPIOumeB_4nhqVa9_nMo4EOE,ng0ij40pxQ47D3WikQ16onGPQdljigEtVrwm436H8tE,viO9ZhxYAtDgbTJ_iNP6RS9trp8Y2HytFSmRqWnWwME","t3":"w9XjgjT9HOJsFh9T05VdGvRyFkzo9PwNbkxJ1oaMCMk,ybcTo5nm_4DbPPIof425GKX1vvUSzPsl8-D99JjlPlY"}}],"parsed":[{"generation":"54GjFaM4zzz","shard":0,"totalShards":1}]},{"peerKey":"857fb10cd3b55c0349d1657ff7c34fcc4c01b44e98621a09f070cf83dacd2995","port":65535,"nonce":"q9x","topics":["topic-18-0","topic-18-1","topic-18-2","topic-18-3","topic-18-4","topic-18-5","topic-18-6","topic-18-7","topic-18-8","topic-18-9","topic-18-10","topic-18-11","topic-18-12","topic-18-13","topic-18-14","topic-18-15","topic-18-16","topic-18-17"],"records":[{"name":"hyperdht-mdns-857fb10cd3b5-eFJuWd4tq9x-0-1","type":"hyperdht-mdns","protocol":"udp","port":65535,"txt":{"v":"1","peerKey":"857fb10cd3b55c0349d1657ff7c34fcc4c01b44e98621a09f070cf83dacd2995","port":"65535","tc":"18","t0":"-12EAamtXEKMh_zuJobsX9R7CPQwz_Jsp4UM_yuFLTE,1rJj7AyCj2LytXDKiJaPIKXQtnW0SdP5Fel7HzqajEk,3iSsRvl2BpW_vPw9FpXUE9AD5h_-UG_VChlD-9RGePs,50Hx-84CdJYCDFKJ-w_zctVXg8y6GcyFp8JZEvnehOw,7rH9HMdRXzypPDQXTAGFydc_du6fEGmIge44K3VpUGQ","t1":"80qiHBQOSGIsjXX0LxnjJqWJN-Y0lpm54MM7tIhGc_4,8Tnsj-7SrboRRwCgvYAFTzd9Eqt4X44c4l0FQEXYCU4,9UA2tVcJXYRT4_pbrIOGNuq-llKEq5jxRNQiMSb0MlE,BDDoKW4Wi9oVnQxndOcJ0ws4VNRtmr4LqsEPBgFRELw,BwxePGfLYtqKb_t3DriA6usOEDVpWvs2XPNDqEs5f5g","t2":"JqQOIWcYNZG6gaSnpGryJdjPw1U9x41NbPuWUECDlX4,TLlI1TNo1uROPNh9DIVQw7WLdxgMlYAVKu42Jnam6p8,W_Dl1hktJ4fvps3FHqpK5yMPmP62I1xe06NGzrX_vHs,X2KqWhxn4oxi82rNStVSu7WH4IGStE4BONftf6aL3hI,fu4VufSIH-CiGOeesCDjWpJCX4LggOEv4wRs2KYQJEA","t3":"gqbEsfoYFCV8eajGKWjs-mcXcbDTD7KKqfmVNlFlJBw,wK61H-OBnHem3AtT6gVuE-e4wQX6UI1NcPcBYeqsM3s,xZemiH3h7J2RL8ak5J_lhyMEPZfdETXmyMJs1_t_5tI"}}],"parsed":[{"generation":"eFJuWd4tq9x","shard":0,"totalShards":1}]},{"peerKey":"befe934be226e6ff356b01fc6af20f604b6f0bcdf0f898acb522eccebc29f8c2","port":8080,"nonce":"k7m","topics":["topic-19-0","topic-19-1","topic-19-2","topic-19-3","topic-19-4","topic-19-5","topic-19-6","topic-19-7","topic-19-8","topic-19-9","topic-19-10","topic-19-11","topic-19-12","topic-19-13","topic-19-14","topic-19-15","topic-19-16","topic-19-17","topic-19-18"],"records":[{"name":"hyperdht-mdns-befe934be226-Oz9_PCI_k7m-0-2","type":"hyperdht-mdns","protocol":"udp","port":8080,"txt":{"v":"1","peerKey":"befe934be226e6ff356b01fc6af20f604b6f0bcdf0f898acb522eccebc29f8c2","port":"8080","tc":"18","t0":"0_I-fKhzzVrNgCEv16WbZ5iRGCjbqiho6lYVf4opY8s,1QKJaQH4_nYRxmIHJhQ1kh8_5bxyxyuqlc7vCmOmh_0,1XCtSYznK-iNHcI0_mCGfKPXofs7XHwffFTaSuRk4qI,4_Dlz9lbhAWbEvilHReuNtcizrz6hF2BJRG1bvTteMo,8P2mKubsRuGwJRr792cE4yVePT5rqsM8n3DUVHiXedc","t1":"8sFFj3JbS5AI6OxaWalcOzBdShIidlxTAfHMxk_uaOc,HTldAu9tu5kDf3SSBX6POQkPM54H-recAWPUI69pghU,ICBHPgwxpjjcCX70fdfLQuKXlorPj0gStGA_8D5QsDQ,LUycpMfftkSzIiN7MOFp6Q1zndw3ivUcmZMN7vtW018,OoJ-hJI_IvPhPB9Lf82x0Lu-4iFXKrDPgQhU-mxtOuA","t2":"RF-SJTvfEsbUAszGvuKtbSpW9C-ijqGYkl-xGt_lZ_A,U1A_CEGSMzuPOCQRaa81BWg0226zNC4QdmrxBBUiEtM,YQRJx_Iyy886YLDqRj91_LOEVBGg-c8QnGdggAj_nUg,aJ7nq2zVWnqFIy8Kudv9mayJaSnmpP9PfuURbW9FW_0,d0vF2_oEn-RrUGMaQ3vBJt6OfAAWUcoYaA9WpFFBQ8I","t3":"qn_B9of6ZO5oWBXgIIHJCZY4Xfi55qn9new8FO8VDd0,sRn7oLb3URy_MRPgoA4Yuiai1sbxxGMyaFEbe8PG_v4,vRh5yvlLBDG1l4ZNYgLdxKgDHO9XiKfgHAmTvdoTAyw"}},{"name":"hyperdht-mdns-befe934be226-Oz9_PCI_k7m-1-2","type":"hyperdht-mdns","protocol":"udp","port":8080,"txt":{"v":"1","peerKey":"befe934be226e6ff356b01fc6af20f604b6f0bcdf0f898acb522eccebc29f8c2","port":"8080","tc":"1","t0":"ymtlFNMZRFRaxhMB8120yWWCh8eD38Xuu1Y8XT4-dls"}}],"parsed":[{"generation":"Oz9_PCI_k7m","shard":0,"totalShards":2},{"generation":"Oz9_PCI_k7m","shard":1,"totalShards":2}]}],"negative":[{"name":"v2","servicePort":4100,"txt":{"v":"2","peerKey":"996726fde9880a4bb8971c9882f39d992f9671a2b9a11a67f628bce8d24a2c0e","port":"4100","tc":"0"},"parsed":null},{"name":"short-peer-key","servicePort":4100,"txt":{"v":"1","peerKey":"abcd","port":"4100","tc":"0"},"parsed":null},{"name":"txt-port-ignored-when-service-port-set","servicePort":4100,"txt":{"v":"1","peerKey":"22181b5622a0810ae6bdc8c87af462fe7cec4efef41d8762cab8ab50f37d959d","port":"0","tc":"0"},"parsed":{"peerKeyHex":"22181b5622a0810ae6bdc8c87af462fe7cec4efef41d8762cab8ab50f37d959d","peerKey":{"type":"Buffer","data":[34,24,27,86,34,160,129,10,230,189,200,200,122,244,98,254,124,236,78,254,244,29,135,98,202,184,171,80,243,125,149,157]},"port":4100,"tokens":[],"generation":null,"shard":0,"totalShards":1}},{"name":"txt-port-used-when-service-port-missing","servicePort":0,"txt":{"v":"1","peerKey":"1ff0426ef9d9d5763629f9d2d02f0f76dd46324057034dc826ac633418b0e65f","port":"4500","tc":"0"},"parsed":{"peerKeyHex":"1ff0426ef9d9d5763629f9d2d02f0f76dd46324057034dc826ac633418b0e65f","peerKey":{"type":"Buffer","data":[31,240,66,110,249,217,213,118,54,41,249,210,208,47,15,118,221,70,50,64,87,3,77,200,38,172,99,52,24,176,230,95]},"port":4500,"tokens":[],"generation":null,"shard":0,"totalShards":1}},{"name":"no-port-anywhere","servicePort":0,"txt":{"v":"1","peerKey":"976e30c66ad1f6e9e12a8600339720ccfd31ca5bea0758acea438e92c53909c2","port":"0","tc":"0"},"parsed":null},{"name":"tc-too-big","servicePort":4100,"txt":{"v":"1","peerKey":"d030d80de18675427b43292977e0cb2e16f4f2e488e2432a1ba85bb3b7d87fc0","port":"4100","tc":"33"},"parsed":null},{"name":"missing-chunk","servicePort":4100,"txt":{"v":"1","peerKey":"db2a51d4512b99e82d88c60b4fffc540159c410ed14e6a15ff95285cbd163380","port":"4100","tc":"2","t0":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"parsed":null},{"name":"short-token","servicePort":4100,"txt":{"v":"1","peerKey":"95244edef41e49337c339efb5f290f0028797efdb312789d8853e48110effbc4","port":"4100","tc":"1","t0":"tooshort"},"parsed":null},{"name":"tc-mismatch","servicePort":4100,"txt":{"v":"1","peerKey":"81ab105d3017840158b7e38e74be74803d6edf376dded513b1db2fd67bdb0b6e","port":"4100","tc":"1","t0":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},"parsed":null}]}"##;

fn fixture() -> Value {
    serde_json::from_str(FIXTURE).expect("embedded fixture")
}

/// TXT object from the fixture in its original key order.
fn txt_pairs(value: &Value) -> Vec<(String, String)> {
    value
        .as_object()
        .expect("txt object")
        .iter()
        .map(|(k, v)| (k.clone(), v.as_str().expect("txt value").to_string()))
        .collect()
}

fn tokens_of(value: &Value) -> Vec<String> {
    value
        .as_array()
        .expect("token array")
        .iter()
        .map(|v| v.as_str().expect("token").to_string())
        .collect()
}

fn str_of(value: &Value, key: &str) -> String {
    value[key]
        .as_str()
        .unwrap_or_else(|| panic!("{key}"))
        .to_string()
}

fn sorted_tokens(topics: &[String]) -> Vec<String> {
    let mut tokens: Vec<String> = topics.iter().map(|t| topic_token(t.as_bytes())).collect();
    tokens.sort_unstable();
    tokens
}

#[test]
fn js_fixture_parity() {
    let fixture = fixture();
    let cases = fixture["cases"].as_array().expect("cases");
    assert_eq!(cases.len(), 20);

    for case in cases {
        let peer_key = str_of(case, "peerKey");
        let port = case["port"].as_u64().expect("port") as u16;
        let nonce = str_of(case, "nonce");
        let topics: Vec<String> = case["topics"]
            .as_array()
            .expect("topics")
            .iter()
            .map(|t| t.as_str().expect("topic").to_string())
            .collect();

        // Token encoding must match, in the order the shards carry them.
        let expected_records = case["records"].as_array().expect("records");
        let expected_parsed = case["parsed"].as_array().expect("parsed");
        let mut app_tokens: Vec<String> = Vec::new();
        for record in expected_records {
            let txt = txt_pairs(&record["txt"]);
            let tc: usize = txt
                .iter()
                .find(|(k, _)| k == "tc")
                .expect("tc")
                .1
                .parse()
                .unwrap();
            for i in 0..tc.div_ceil(5) {
                let chunk = txt
                    .iter()
                    .find(|(k, _)| k == &format!("t{i}"))
                    .unwrap_or_else(|| panic!("t{i} missing"))
                    .1
                    .clone();
                app_tokens.extend(chunk.split(',').map(str::to_owned));
            }
        }
        assert_eq!(app_tokens, sorted_tokens(&topics), "{topics:?}");

        let built = create_records_with(&peer_key, port, &sorted_tokens(&topics), &nonce);
        assert_eq!(built.len(), expected_records.len(), "{topics:?}");
        for (ours, theirs) in built.iter().zip(expected_records) {
            assert_eq!(ours.name, str_of(theirs, "name"));
            assert_eq!(ours.port, port);
            assert_eq!(ours.txt, txt_pairs(&theirs["txt"]));
        }

        // And the app's verdict on each record must be ours.
        for ((record, parsed), ours) in expected_records.iter().zip(expected_parsed).zip(&built) {
            let txt = txt_pairs(&record["txt"]);
            let name = str_of(record, "name");
            let app =
                parse_record(&name, port, &txt).unwrap_or_else(|| panic!("app parsed {name}"));
            let mine = ours.parse().expect("built record parses");
            assert_eq!(app, mine, "{name}");
            assert_eq!(app.peer_key_hex, peer_key);
            assert_eq!(app.port, port);
            assert_eq!(
                Some(mine.generation.clone().unwrap()),
                parsed["generation"].as_str().map(str::to_owned)
            );
            assert_eq!(mine.shard, parsed["shard"].as_u64().unwrap() as usize);
            assert_eq!(
                mine.total_shards,
                parsed["totalShards"].as_u64().unwrap() as usize
            );
        }
    }

    for case in fixture["negative"].as_array().expect("negative") {
        let name = str_of(case, "name");
        let service_port = case["servicePort"].as_u64().expect("servicePort") as u16;
        let txt = txt_pairs(&case["txt"]);
        let expected = &case["parsed"];
        match parse_record(&name, service_port, &txt) {
            None => assert!(expected.is_null(), "{name} should have parsed"),
            Some(parsed) => {
                assert!(!expected.is_null(), "{name} should have been rejected");
                assert_eq!(
                    parsed.peer_key_hex,
                    str_of(expected, "peerKeyHex"),
                    "{name}"
                );
                assert_eq!(
                    parsed.port,
                    expected["port"].as_u64().unwrap() as u16,
                    "{name}"
                );
                assert_eq!(parsed.tokens, tokens_of(&expected["tokens"]), "{name}");
            }
        }
    }
}

// ── Live interop ────────────────────────────────────────────────────────────

const RUST_PORT: u16 = 19444;
const NODE_PORT: u16 = 19445;
const WAIT: Duration = Duration::from_secs(30);

fn manifest_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// The address other hosts would reach us on; a connected UDP socket only asks the routing table.
fn local_ipv4() -> Option<Ipv4Addr> {
    for probe in ["224.0.0.251:5353", "1.1.1.1:53", "192.0.2.1:9"] {
        let sock = UdpSocket::bind("0.0.0.0:0").ok()?;
        if sock.connect(probe).is_ok()
            && let Ok(local) = sock.local_addr()
            && let IpAddr::V4(v4) = local.ip()
        {
            return Some(v4);
        }
    }
    None
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Event {
    Up,
    Down,
}

struct Node {
    child: Child,
    lines: Receiver<Value>,
}

impl Node {
    fn spawn(script: &PathBuf, config: &Value) -> Self {
        let mut child = Command::new("node")
            .arg(script)
            .arg("interop")
            .arg(config.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn node (run `npm install` in tests/js first)");
        let stdout = child.stdout.take().expect("node stdout");
        let (tx, lines) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                if let Ok(value) = serde_json::from_str::<Value>(&line) {
                    let stop = value["t"] == "error";
                    let _ = tx.send(value);
                    if stop {
                        break;
                    }
                }
            }
        });
        Self { child, lines }
    }

    fn send(&mut self, line: &str) {
        let stdin = self.child.stdin.as_mut().expect("node stdin");
        writeln!(stdin, "{line}").expect("write to node");
        stdin.flush().expect("flush node stdin");
    }

    fn wait_for(&self, what: &str, mut matches: impl FnMut(&Value) -> bool) -> Value {
        let deadline = Instant::now() + WAIT;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            match self.lines.recv_timeout(left.max(Duration::from_millis(1))) {
                Ok(value) if value["t"] == "error" => panic!("node reported: {value}"),
                Ok(value) if matches(&value) => return value,
                Ok(_) => {}
                Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => {
                    panic!("timed out waiting for {what}")
                }
            }
        }
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn await_event(
    events: &Receiver<(Event, DiscoveredService)>,
    what: &str,
    mut matches: impl FnMut(Event, &DiscoveredService) -> bool,
) -> DiscoveredService {
    let deadline = Instant::now() + WAIT;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        match events.recv_timeout(left.max(Duration::from_millis(1))) {
            Ok((kind, service)) if matches(kind, &service) => return service,
            Ok(_) => {}
            Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => {
                panic!("timed out waiting for {what}")
            }
        }
    }
}

#[test]
#[ignore = "needs a working multicast DNS path between a Node and a Rust process; run with --include-ignored"]
fn mdns_interop_with_bonjour_service() {
    let script = manifest_dir().join("tests/js/lan-mdns.mjs");
    assert!(
        script
            .with_file_name("node_modules/bonjour-service")
            .exists(),
        "run `npm install` in peartube-native/tests/js"
    );
    let Some(address) = local_ipv4() else {
        panic!("this machine has no IPv4 route to publish on");
    };

    let node_topics = vec!["node-alpha".to_string(), "node-beta".to_string()];
    let node_key = {
        use sha2::{Digest, Sha256};
        hex(&Sha256::digest(b"peartube-node-lan-peers")[..])
    };
    // A deterministic key keeps failure output readable; it never collides with the node's key.
    let rust_key = {
        use sha2::{Digest, Sha256};
        hex(&Sha256::digest(b"peartube-rust-lan-peers")[..])
    };
    let rust_topics = vec![
        "rust-alpha".to_string(),
        "rust-beta".to_string(),
        "rust-gamma".to_string(),
    ];
    assert_ne!(node_key, rust_key);

    let config = serde_json::json!({
        "peerKey": node_key,
        "port": NODE_PORT,
        "topics": node_topics,
        "nonce": "n0d",
    });
    let mut node = Node::spawn(&script, &config);

    let adapter = SelfAddressAdapter::with_rebrowse(
        peartube_native::lan::mdns::BonjourAdapter::new(),
        address,
        Some(Duration::from_secs(2)),
    );
    let on_error: ErrorSink = Arc::new(|message| eprintln!("mdns error: {message}"));

    // Rust publishes a PearTube record; the Node side must parse it into the same fields.
    let peer_key = {
        use sha2::{Digest, Sha256};
        Sha256::digest(b"peartube-rust-lan-peers")
    };
    let record = create_records_with(
        &hex(&peer_key[..]),
        RUST_PORT,
        &sorted_tokens(&rust_topics),
        "r5t",
    )
    .into_iter()
    .next()
    .expect("one shard");
    let advertisement = adapter
        .advertise(&record, Some(Arc::clone(&on_error)))
        .expect("advertise");
    node.wait_for("node published", |value| value["t"] == "published");

    let node_sees_rust = node.wait_for("node up for the rust record", |value| {
        value["t"] == "up" && value["peerKey"] == rust_key.as_str()
    });
    assert_eq!(node_sees_rust["port"].as_u64(), Some(u64::from(RUST_PORT)));
    let seen_topics: Vec<String> = node_sees_rust["topics"]
        .as_array()
        .expect("topics")
        .iter()
        .map(|t| t.as_str().unwrap().to_string())
        .collect();
    assert_eq!(seen_topics, sorted_tokens(&rust_topics));
    assert_eq!(
        node_sees_rust["host"].as_str(),
        Some(address.to_string().as_str())
    );

    // Rust browses and must resolve the Node record the same way.
    let (tx, events) = mpsc::channel();
    let up_tx = tx.clone();
    let on_up: ServiceSink = Arc::new(move |service: &DiscoveredService| {
        let _ = up_tx.send((Event::Up, service.clone()));
    });
    let down_tx = tx.clone();
    let on_down: ServiceSink = Arc::new(move |service: &DiscoveredService| {
        let _ = down_tx.send((Event::Down, service.clone()));
    });
    let browse = adapter
        .browse(
            Arc::clone(&on_up),
            Some(Arc::clone(&on_down)),
            Some(Arc::clone(&on_error)),
        )
        .expect("browse");

    let rust_sees_node = await_event(&events, "rust up for the node record", |kind, service| {
        kind == Event::Up && service.parse().is_some_and(|p| p.peer_key_hex == node_key)
    });
    let parsed = rust_sees_node.parse().expect("node record parses");
    assert_eq!(parsed.port, NODE_PORT);
    assert_eq!(parsed.tokens, sorted_tokens(&node_topics));
    assert_eq!(rust_sees_node.port, NODE_PORT);

    // Withdrawing the Rust advertisement is a goodbye the Node side reports as down.
    advertisement.stop();
    node.wait_for("node down for the rust record", |value| {
        value["t"] == "down" && value["peerKey"] == rust_key.as_str()
    });

    // And the Node withdrawing its record is a down for the Rust browse.
    node.send("stop-publish");
    node.wait_for("node unpublished", |value| value["t"] == "unpublished");
    await_event(&events, "rust down for the node record", |kind, service| {
        kind == Event::Down && service.parse().is_some_and(|p| p.peer_key_hex == node_key)
    });

    browse.stop();
    node.send("exit");
}
