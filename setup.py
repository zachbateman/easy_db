import setuptools

with open('README.md', 'r') as f:
    long_description = f.read()

setuptools.setup(
    name='easy_db',
    version='0.9.2',
    packages=['easy_db'],
    license='MIT',
    author='Zach Bateman',
    description='Easy Python database interaction',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/zachbateman/easy_db.git',
    download_url='https://github.com/zachbateman/easy_db/archive/v_0.9.2.tar.gz',
    keywords=['DATABASE', 'SIMPLE', 'EASY'],
    install_requires=['pyodbc', 'tqdm'],
    classifiers=['Development Status :: 4 - Beta',
                   'License :: OSI Approved :: MIT License',
                   'Programming Language :: Python :: 3',
                   'Programming Language :: Python :: 3.7',
                   'Programming Language :: Python :: 3.8',
                   'Programming Language :: Python :: 3.9',
                   ]
)
