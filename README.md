# EE 547 Homework 2 (Spring 2026)

## q1

In this part, I wrote a Python program that reads a list of web addresses and visits each one. For every page, the program records basic information such as whether the request succeeded, how long it took, and how much data was returned. Any failures are logged without stopping the program. The script runs inside a Docker container, and input and output files are shared with the host machine using mounted folders.

## q2

This part works with research paper data from the ArXiv website. The program sends a search request, reads the response, and extracts basic information about each paper, including the title, authors, and abstract. It also performs simple analysis on the abstract text and saves both individual paper data and overall results to output files. The entire program is packaged and run inside a Docker container.

## q3

In the final part, I built a small pipeline using three Docker containers that run one after another. The first container downloads web pages, the second cleans and summarizes the text, and the third combines everything into a final report. All containers share the same folder to pass data between steps, and each stage waits for the previous one to finish before starting. The full pipeline can be run with a single command using Docker Compose.
