// fs = nodejs core module: file system
// events = will use event emitters to understand when partial sorting is complete
// util = required module for event emitters
// myEmitter = will be used to emit when important parts of the applications are finished
const fs = require('fs'),
	EventEmitter = require('events'),
	util = require('util'),
	myEmitter;

var externalSort = (function () {
	// below are main functions and global variables used in this solution:
	// sort = main function to be called. only this one will be visible to outside.
	// myEmitter = will be used to catch three events: quicksort complete, files read, merge sort complete.
	// readPartialThenSortAndWrite = used to tackle with the asynchronous nature of nodejs.
	// readChunk = read limited file data (~ like 10 mb).
	// quicksort = after the file is processed into seperate files, this method will sort them.
	// kWayMergeSort = after all files are sorted, they will all be merged using this method.
	// binaryMerge = will be used in kWayMergeSort algorithm.
	// swapTwoElementsInArray = as the name suggests, swaps two elements in an array. Used in quicksort.
	// appendThisToFileName = will be used to name the newly created smaller chunks of the big file.
	// countRecursions = variable used to count number of recursions in quicksort.
	// countSplitFiles = number of files needed to divide the big file. Will be used to push an event when quick sorting is done.
	// splitFiles = file names added after big file sorted into smaller chunks.
	// chunkSize = size we will read from a file in every sequence.
	// path = path of the big file.
	// taking our limited memory size as 100 megabytes.
	var sort, myEmitter, readPartialThenSortAndWrite, readChunk, quickSort, mergeSort, kWayMergeSort, binaryMerge, swapTwoElementsInArray, appendThisToFileName, countRecursions = 0, countSplitFiles = 0, splitFiles = [], chunkSize, path, memory = 100, memoryInBytes = memory * 1000000.0;

	function MyEmitter() {
		EventEmitter.call(this);
	}
	util.inherits(MyEmitter, EventEmitter);
	myEmitter = new MyEmitter();
	
	// after quicksort finishes, start reading files.
	myEmitter.on('quicksort finished', () => {
		// mainArray = holds 100 mb of ram data
		// fileDepo = holds file names plus some necessary info
		// readThisFileNext = counting which file to read next
		var mainArray = [], fileDepo = [], readThisFileNext, i;
		chunkSize = memoryInBytes / splitFiles.length;
		countSplitFiles = splitFiles.length;
		for (i = 0; i < countSplitFiles; i++) {
			fileDepo.push({path: splitFiles[i], currentPosition: 0});
		}
		readThisFileNext = 0;
		myEmitter.emit('read chunk', mainArray, readThisFileNext, fileDepo);
	});

	myEmitter.on('read chunk', (mainArray, readThisFileNext, fileDepo) => {
		readChunk(mainArray, fileDepo[readThisFileNext].path, fileDepo[readThisFileNext].currentPosition, fileDepo[readThisFileNext].currentPosition + chunkSize, readThisFileNext, fileDepo);
	});

	readChunk = function (mainArray, path, start, end, readThisFileNext, fileDepo) {
		var sortedData, sortedDataInArray = [], readableStream;
		readableStream = fs.createReadStream(path, {start: start, end: end});
		readableStream.on('data', function (v) {
			if (v) {
				mainArray += v;
			}
		});
		readableStream.on('end', function() {
			fileDepo[readThisFileNext].currentPosition = fileDepo[readThisFileNext].currentPosition + chunkSize;
			if (readThisFileNext + 1 === fileDepo.length) {
				myEmitter.emit('start kway merge sort', mainArray, fileDepo);
			} else {
				readThisFileNext++;
				readChunk(mainArray, fileDepo[readThisFileNext].path, fileDepo[readThisFileNext].currentPosition, fileDepo[readThisFileNext].currentPosition + chunkSize, readThisFileNext, fileDepo);
			}
		});
	}

	myEmitter.on('start kway merge sort', (mainArray, fileDepo) => {
		var writableStream;
		mainArray = mainArray.toString().split("\n");

		quickSort(mainArray, 0, mainArray.length, function(data) {
			//writableStream = fs.appendFile(appendThisToFileName(path, '_sorted'), data);
			writableStream = fs.createWriteStream(appendThisToFileName(path, '_sorted'), {'flags': 'a'});
			data.forEach(function(v) { writableStream.write(v + '\n'); });
			writableStream.end();
			writableStream.on('finish', () => {
				if (fileDepo[0].currentPosition < memoryInBytes) {
					readThisFileNext = 0;
					data = [];
					readChunk(data, fileDepo[readThisFileNext].path, fileDepo[readThisFileNext].currentPosition, fileDepo[readThisFileNext].currentPosition + chunkSize, readThisFileNext, fileDepo);
				} else {
					console.log('finished');
					return;
				}
			});
		});

		//mergeSort(mainArray, function(data) {
		//writableStream = fs.appendFile(appendThisToFileName(path, '_sorted'), data);
		//writableStream = fs.createWriteStream(appendThisToFileName(path, '_sorted'), {'flags': 'a'});
		//data.forEach(function(v) { writableStream.write(v + '\n'); });
		//writableStream.end();
		//writableStream.on('finish', () => {
		//if (fileDepo[0].currentPosition < fs.statSync(fileDepo[0].path)['size']) {
		//readThisFileNext = 0;
		//data = [];
		//readChunk(data, fileDepo[readThisFileNext].path, fileDepo[readThisFileNext].currentPosition, fileDepo[readThisFileNext].currentPosition + chunkSize, readThisFileNext, fileDepo);
		//} else {
		//console.log('finished');
		//return;
		//}
		//});
		//});
	});

	sort = function (fileName) {
		path = fileName;
		var i, stats = fs.statSync(fileName), 
			fileSizeInBytes = stats['size'],
			numberOfSplitFilesNeeded = Math.floor(fileSizeInBytes / memoryInBytes),
			currentFileSize = 0,
			splitFile;
			path = fileName;
		countSplitFiles = numberOfSplitFilesNeeded;
		for (i = 0; i <= numberOfSplitFilesNeeded; i++) {
			currentFileSize = i * memoryInBytes;
			readPartialThenSortAndWrite(i, fileName, currentFileSize, currentFileSize + memoryInBytes);
		}
	};

	mergeSort = function (data, callback) {
		callback(kWayMergeSort(data));
	}

	kWayMergeSort = function (data) {
		var length = data.length,
			mid,
		leftHalf,
		rightHalf;
		if (data.length === 1) {
			return data;
		}
		mid = Math.floor(length * 0.5);
		leftHalf = data.slice(0, mid);
		rightHalf = data.slice(mid, length);
		return binaryMerge(kWayMergeSort(leftHalf), kWayMergeSort(rightHalf));
	}

	binaryMerge = function (left, right) {
		var result = [];
		while (left.length || right.length) {
			if (left.length && right.length) {
				if (left[0].toLowerCase() < right[0].toLowerCase()) {
					result.push(left.shift());
				} else {
					result.push(right.shift());
				}
			} else if (left.length) {
				result.push(left.shift());
			} else {
				result.push(right.shift());
			}
		}
		return result;
	}

	readPartialThenSortAndWrite = function (i, path, start, end) {
		var dataToSortWithQuickSort, dataToSortWithQuickSortInArray = [],
			readableStream, file;
		readableStream = fs.createReadStream(path, {start: start, end: end});
		readableStream.on('data', function (data) {
			if (data) {
				dataToSortWithQuickSort += data;
			}
		});
		readableStream.on('end', function() {
			dataToSortWithQuickSortInArray = dataToSortWithQuickSort.toString().split("\n");
			quickSort(dataToSortWithQuickSortInArray, 0, dataToSortWithQuickSortInArray.length, function(data) {
				file = fs.createWriteStream(appendThisToFileName(path, i));
				splitFiles.push(appendThisToFileName(path, i));
				data.forEach(function(v) { file.write(v + '\n'); });
				file.end();
				file.on('finish', () => {
					countSplitFiles--;
					file.end();
					if (countSplitFiles === 0) {
						console.log('quicksort finished');
						myEmitter.emit('quicksort finished');
					}
				});
			});
		});
	}

	// using hoare partition scheme.
	// taking the middle element as pivot since the data 
	// is highly random. 	
	quickSort = function (data, startIndex, endIndex, writeToFile) {
		var i = startIndex, j = endIndex, pivotPosition, pivot;
		pivotPosition = Math.floor((startIndex + endIndex) / 2);
		if (data[pivotPosition]) {
			pivot = data[pivotPosition].toLowerCase();
		}
		countRecursions++;
		do {
			while (data[i] && data[i].toLowerCase() < pivot) {
				i++;
			}
			while (data[j] && data[j].toLowerCase() > pivot) {
				j--;
			}
			if (i <= j) {
				swapTwoElementsInArray(data, i, j);
				i++;
				j--;
			}
		} 
		while (i <= j);
		if (startIndex < j) {
			quickSort(data, startIndex, j);
		}
		if (i < endIndex) {
			quickSort(data, i, endIndex);
		}
		countRecursions--;
		if (countRecursions === 0) {
			writeToFile(data);
		}
	};

	swapTwoElementsInArray = function (array, fromIndex, toIndex) {
		var element = array[fromIndex];
		array[fromIndex] = array[toIndex];
		array[toIndex] = element;
	}

	appendThisToFileName = function (fileName, number) {
		var indexOfDot = fileName.indexOf('.'), fileNameBeforeDot = fileName.slice(0, indexOfDot),
			fileExtension = fileName.slice(indexOfDot, fileName.length);
		return fileNameBeforeDot + number + fileExtension;
	}

	return {
		sort: sort
	};
})();


externalSort.sort('file.txt');
