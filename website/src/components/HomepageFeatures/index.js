import React from 'react';
import clsx from 'clsx';
import styles from './styles.module.css';

const FeatureList = [

    {
        title: 'Unified Data Analytics Platform',
        Svg: require('@site/static/img/main_page_01.svg').default,
        description: (
            <>
				Support data analytics from different platform including Hadoop/Cloud/RDBMS,
				while providing a unified interface for downstream applications.
            </>
        ),
    },
    {
        title: 'Native Compute Engine',
        Svg: require('@site/static/img/main_page_02.svg').default,
        description: (
            <>
                Use Native Engine to enable vector acceleration and cpu instruction level optimization.
                <br></br> Gluten and Datafusion will be integrated into Kylin. (This task is working in process.)
            </>
        ),
    },
    {
        title: 'Cloud Native Architecture',
        Svg: require('@site/static/img/main_page_03.svg').default,
        description: (
            <>
                Support deployment on K8S and use a separate storage and
                computing architecture and allow the elastic scaling of resources.
            </>
        ),
    },
    {
        title: 'Support BI/Excel',
        Svg: require('@site/static/img/main_page_04.svg').default,
        description: (
            <>
                Support connecting to different BI tools, like Tableau/Power BI/Excel.
            </>
        ),
    },
    {
		title: 'Brand New Frontend',
		Svg: require('@site/static/img/main_page_05.svg').default,
		description: (
			<>
				New modeling process are concise by letting user define table
				relationship/dimensions/measures in a single canvas.
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
