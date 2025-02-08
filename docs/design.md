# Book to Quiz Generator Design Document

## Overview
An application that converts books into markdown format and generates educational question-answer pairs using Google's Gemini API.

## Core Components

### Text Extraction Module
- AI basd PDF to Markdown
- [marker](https://github.com/VikParuchuri/marker)

### Content Processing
- Text chunking algorithm to break content into digestible segments
- Metadata preservation for chapter/section tracking
- Section marker injection for better context

### Question Generation
- Gemini API integration for Q&A generation
- Prompt templates optimized for educational content
- Rate limiting and batch processing for API calls


