import React from 'react';
import clsx from 'clsx';
import styles from './styles.module.css';

const FeatureList = [
    {
        title: 'Brand New Frontend',
        Svg: require('@site/static/img/undraw_docusaurus_react.svg').default,
        description: (
            <>
                Kylin has a brand new frontend now. Everything in Kylin becomes easier and quicker. Such as building models, query data, load data sources, checking job status, configuration, etc.
            </>
        ),
    },
    {
        title: 'Unified Data Analytics Platform',
        Svg: require('@site/static/img/undraw_docusaurus_react.svg').default,
        description: (
            <>
               Support data acquisition from different platforms including streaming data, databases, data lakes, and clouds, all while providing a unified interface for downstream applications.
            </>
        ),
    },
    {
        title: 'Native Compute Engine',
        Svg: require('@site/static/img/undraw_docusaurus_react.svg').default,
        description: (
            <>
                Kylin provides a native compute engine offering sub-second response times for standard SQL queries on petabyte-scale datasets.
            </>
        ),
    },
    {
        title: 'Cloud Native Architecture',
        Svg: require('@site/static/img/undraw_docusaurus_react.svg').default,
        description: (
            <>
                Support on-premise, public, and private cloud deployments. Use a separate storage and computing architecture and allow the elastic scaling of computing resources.
            </>
        ),
    },
    {
        title: 'Support MDX',
        Svg: require('@site/static/img/undraw_docusaurus_react.svg').default,
        description: (
            <>
                Map complex data into business terms. Run as a service for every consumer with universal data definitions and industry-standard interfaces at the petabyte scale.
            </>
        ),
    },
];

function Feature({Svg, title, description}) {
    return (
        <div className={clsx('col col--4')}>
            <div className="text--center">
                <Svg className={styles.featureSvg} role="img"/>
            </div>
            <div className="text--center padding-horiz--md">
                <h3>{title}</h3>
                <p>{description}</p>
            </div>
        </div>
    );
}

export default function HomepageFeatures() {
    return (
        <section className={styles.features}>
            <div className="container">
                <div className="row">
                    {FeatureList.map((props, idx) => (
                        <Feature key={idx} {...props} />
                    ))}
                </div>
            </div>
        </section>
    );
}
