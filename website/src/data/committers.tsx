import React from 'react';

import {CommitterItem} from "../components/committers";

const COMMITTERS: CommitterItem[] = [
    {
        name: 'George Song (宋轶)',
        role: 'PMC',
        org: 'SAP',
        apacheId: 'yisong',
        githubId: 'songyi10011001',
        showOnPages: true
    },
    {
        name: 'Henry Saputra',
        role: 'Mentor',
        org: '',
        apacheId: 'hsaputra',
        githubId: 'hsaputra',
        showOnPages: true
    },
    {
        name: 'Hongbin Ma (马洪宾)',
        role: 'PMC',
        org: 'Kyligence Inc.',
        apacheId: 'mahongbin',
        githubId: 'binmahone',
        showOnPages: true
    },
    {
        name: 'Jason Zhong (仲俭)',
        role: 'PMC',
        org: 'Kyligence Inc.',
        apacheId: 'zhongjian',
        githubId: 'janzhongi',
        showOnPages: true
    },
    {
        name: 'Julian Hyde',
        role: 'Mentor',
        org: 'Hortonworks',
        apacheId: 'jhyde',
        githubId: 'julianhyde',
        showOnPages: true
    },
    {
        name: 'Luke Han (韩卿)',
        role: 'PMC',
        org: 'Kyligence Inc.',
        apacheId: 'lukehan',
        githubId: 'lukehan',
        showOnPages: true
    },
    {
        name: 'Owen O\'Malley',
        role: 'Mentor',
        org: 'Hortonworks',
        apacheId: 'omalley',
        githubId: 'omalley',
        showOnPages: true
    },
    {
        name: 'P. Taylor Goetz',
        role: 'Mentor',
        org: '',
        apacheId: 'ptgoetz',
        githubId: 'ptgoetz',
        showOnPages: true
    },
    {
        name: 'Qianhao Zhou (周千昊)',
        role: 'PMC',
        org: '',
        apacheId: 'qhzhou',
        githubId: 'qhzhou',
        showOnPages: true
    },
    {
        name: 'Ted Dunning',
        role: 'Champion',
        org: 'MapR',
        apacheId: 'tdunning',
        githubId: 'tdunning',
        showOnPages: true
    },
    {
        name: 'Shaofeng Shi (史少锋)',
        role: 'PMC',
        org: 'Kyligence Inc.',
        apacheId: 'shaofengshi',
        githubId: 'shaofengshi',
        showOnPages: true
    },
    {
        name: 'Xiaodong Duo (朵晓东)',
        role: 'Emeritus PMC',
        org: 'Alipay',
        apacheId: 'xduo',
        githubId: 'xduo',
        showOnPages: true
    },
    {
        name: 'Ankur Bansal',
        role: 'Emeritus PMC',
        org: 'eBay',
        apacheId: 'abansal',
        githubId: 'abansal',
        showOnPages: true
    },
    {
        name: 'Xu Jiang (蒋旭)',
        role: 'PMC',
        org: 'Alibaba',
        apacheId: 'jiangxu',
        githubId: 'jiangxuchina',
        showOnPages: true
    },
    {
        name: 'Yang Li (李扬)',
        role: 'PMC Chair',
        org: 'Kyligence Inc.',
        apacheId: 'liyang',
        githubId: 'liyang-gmt8',
        showOnPages: true
    },
    {
        name: 'Dayue Gao (高大月)',
        role: 'PMC',
        org: 'Meituan',
        apacheId: 'gaodayue',
        githubId: 'gaodayue',
        showOnPages: true
    },
    {
        name: 'Hua Huang (黄桦)',
        role: 'PMC',
        org: 'Alibaba',
        apacheId: 'hhuang',
        githubId: 'superhua',
        showOnPages: true
    },
    {
        name: 'Dong Li (李栋)',
        role: 'PMC',
        org: 'Kyligence Inc.',
        apacheId: 'lidong',
        githubId: 'lidongsjtu',
        showOnPages: true
    },
    {
        name: 'Xiaoyu Wang (王晓雨)',
        role: 'PMC',
        org: '',
        apacheId: 'wangxiaoyu',
        githubId: 'xiaowangyu',
        showOnPages: true
    },
    {
        name: 'Yerui Sun (孙业锐)',
        role: 'PMC',
        org: 'Meituan',
        apacheId: 'sunyerui',
        githubId: 'sunyerui',
        showOnPages: true
    },
    {
        name: 'Luwei Chen (陈露薇)',
        role: 'committer',
        org: 'Google',
        apacheId: 'chenluwei',
        githubId: 'LouisaChen',
        showOnPages: true
    },
    {
        name: 'Yanghong Zhong (钟阳红)',
        role: 'PMC',
        org: 'eBay',
        apacheId: 'nju_yaho',
        githubId: 'kyotoYaho',
        showOnPages: true
    },
    {
        name: 'Billy Liu (刘一鸣)',
        role: 'PMC',
        org: 'Kyligence Inc.',
        apacheId: 'billyliu',
        githubId: 'yiming187',
        showOnPages: true
    },
    {
        name: 'Kaisen Kang (康凯森)',
        role: 'PMC',
        org: 'Meituan',
        apacheId: 'kangkaisen',
        githubId: 'kangkaisen',
        showOnPages: true
    },
    {
        name: 'Alberto Ramón',
        role: 'committer',
        org: '',
        apacheId: 'alb',
        githubId: 'albertoRamon',
        showOnPages: true
    },
    {
        name: 'Roger Shi (施继承)',
        role: 'committer',
        org: 'Google',
        apacheId: 'sjc',
        githubId: 'rogercloud',
        showOnPages: true
    },
    {
        name: 'Zhixiong Chen (陈志雄)',
        role: 'committer',
        org: 'Kyligence Inc.',
        apacheId: 'chen',
        githubId: 'chenzhx',
        showOnPages: true
    },
    {
        name: 'Cheng Wang (王成)',
        role: 'committer',
        org: 'Kyligence Inc.',
        apacheId: 'wormhole',
        githubId: 'wormholer',
        showOnPages: true
    },
    {
        name: 'Guosheng Lu (陆国圣)',
        role: 'committer',
        org: 'Meituan',
        apacheId: 'boblu',
        githubId: 'luguosheng1314',
        showOnPages: true
    },
    {
        name: 'Jianhua Peng (彭建华)',
        role: 'committer',
        org: '',
        apacheId: 'pengjianhua',
        githubId: 'pengjianhua',
        showOnPages: true
    },
    {
        name: 'Julian Pan (潘俍颀)',
        role: 'committer',
        org: 'eBay Inc.',
        apacheId: 'julianpan',
        githubId: 'sanjulian',
        showOnPages: true
    },
    {
        name: 'Yu Feng (冯宇)',
        role: 'committer',
        org: 'Alibaba',
        apacheId: 'fengyu',
        githubId: 'terry-chelsea',
        showOnPages: true
    },
    {
        name: 'Allen Ma (马刚)',
        role: 'PMC',
        org: 'eBay Inc.',
        apacheId: 'magang',
        githubId: 'allenma',
        showOnPages: true
    },
    {
        name: 'Chunen Ni (倪春恩)',
        role: 'PMC',
        org: 'pinduoduo.com',
        apacheId: 'nic',
        githubId: 'nichunen',
        showOnPages: true
    },
    {
        name: 'Jiatao Tao (陶加涛)',
        role: 'committer',
        org: 'ByteDance',
        apacheId: 'tao',
        githubId: 'aaaaaaron',
        showOnPages: true
    },
    {
        name: 'Chao Long (龙超)',
        role: 'committer',
        org: 'Kyligence Inc.',
        apacheId: 'chao',
        githubId: 'Wayne1c',
        showOnPages: true
    },
    {
        name: 'Shaohui Liu (刘绍辉)',
        role: 'committer',
        org: 'XiaoMi Group.',
        apacheId: 'liushaohui',
        githubId: 'lshmouse',
        showOnPages: true
    },
    {
        name: 'Temple Zhou (周天鹏)',
        role: 'committer',
        org: 'DXY.cn',
        apacheId: 'ztp',
        githubId: 'TempleZhou',
        showOnPages: true
    },
    {
        name: 'Xiaoxiang Yu (俞霄翔)',
        role: 'PMC',
        org: 'Kyligence Inc.',
        apacheId: 'xxyu',
        githubId: 'hit-lacus',
        showOnPages: true
    },
    {
        name: 'Kaige Liu (刘凯歌)',
        role: 'committer',
        org: 'Kyligence Inc.',
        apacheId: 'kaige',
        githubId: 'etherge',
        showOnPages: true
    },
    {
        name: 'Vino Yang (杨华)',
        role: 'committer',
        org: 't3go.cn',
        apacheId: 'vinoyang',
        githubId: 'yanghua',
        showOnPages: true
    },
    {
        name: 'Rupeng Wang (王汝鹏)',
        role: 'committer',
        org: 'Kyligence Inc.',
        apacheId: 'wangrupeng',
        githubId: 'RupengWang',
        showOnPages: true
    },
    {
        name: 'Xiao Chu (初晓)',
        role: 'committer',
        org: 'Didi Chuxing',
        apacheId: 'xiaochu',
        githubId: 'bigxiaochu',
        showOnPages: true
    },
    {
        name: 'Yiming Xu (许益铭)',
        role: 'committer',
        org: 'Alibaba',
        apacheId: 'liuying',
        githubId: 'hn5092',
        showOnPages: true
    },
    {
        name: 'Yaqian Zhang (张亚倩)',
        role: 'committer',
        org: 'Kyligence Inc.',
        apacheId: 'yaqian',
        githubId: 'zhangayqian',
        showOnPages: true
    },
    {
        name: 'Zhichao Zhang (张智超)',
        role: 'committer',
        org: 'Kyligence Inc.',
        apacheId: 'zhangzc',
        githubId: 'zzcclp',
        showOnPages: true
    },
    {
        name: 'Shengjun Zheng (郑生俊)',
        role: 'committer',
        org: 'Youzan',
        apacheId: 'zhengshengjun',
        githubId: 'zhengshengjun',
        showOnPages: true
    },
    {
        name: 'Lu Cao (曹鲁)',
        role: 'committer',
        org: 'Kyligence Inc.',
        apacheId: 'caolu',
        githubId: 'lionelcao',
        showOnPages: true
    },
    {
        name: 'Yinghao Lin (林鹰昊)',
        role: 'committer',
        org: 'Kyligence Inc.',
        apacheId: 'yhcast0',
        githubId: 'yhcast0',
        showOnPages: true
    },
    {
        name: 'Pengfei Zhan (占鹏飞)',
        role: 'committer',
        org: 'Kyligence Inc.',
        apacheId: 'pfzhan',
        githubId: 'pfzhan',
        showOnPages: true
    },
    {
        name: 'Longfei Jiang (蒋龙飞)',
        role: 'committer',
        org: 'Kyligence Inc.',
        apacheId: 'jlfsdtc',
        githubId: 'jlfsdtc',
        showOnPages: true
    }
]

export default COMMITTERS;