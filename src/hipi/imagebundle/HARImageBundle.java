package hipi.imagebundle;

import hipi.image.FloatImage;
import hipi.image.ImageHeader;
import hipi.image.ImageHeader.ImageType;
import hipi.image.io.CodecManager;
import hipi.image.io.ImageDecoder;
import hipi.container.HARIndexContainer;
import hipi.util.HARIndexContainerSorter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.HarFileSystem;
import org.apache.hadoop.fs.Path;

public class HARImageBundle extends AbstractImageBundle {
	private HarFileSystem _harfs = null;
	private FileStatus[] _filesInHar;
	private int _current_image;
	private int _imageCount;

	private FSDataOutputStream _writer;
	private FSDataInputStream _reader;
	private FSDataOutputStream masterIndexStream = null;
	private FSDataOutputStream indexStream = null;
	
	private byte _cacheData[];
	private int _cacheType;
	private ArrayList<HARIndexContainer> indexHash;
	
	public HARImageBundle(Path file_path, Configuration conf) {

		super(file_path, conf);
		System.out.println("1: "+file_path.toString());

		System.out.println("2: "+_file_path.toString());

	}

	@Override
	protected void openForWrite() throws IOException {
		_imageCount = 0;	
		
		indexHash = new ArrayList<HARIndexContainer>();
		System.out.println("3: "+_file_path.toString());
		Path tmpOutputDir = new Path(_file_path.toUri().getPath());
		String partname = "part-0"; //TODO: temp solution... need to figure how to have multiple splits
		Path tmpOutput = new Path(tmpOutputDir, partname);
		try {
			//try to create har file parts
			FileSystem destFs = tmpOutput.getFileSystem(_conf);
			//this was a stale copy
			if (destFs.exists(tmpOutput)) {
				destFs.delete(tmpOutput, false);
			}
			_writer = destFs.create(tmpOutput);
			
		} catch(IOException e) {
			throw new RuntimeException(e);
		}		
	}

	@Override
	protected void openForRead() throws IOException {
	    _harfs = new HarFileSystem(FileSystem.get(_conf));
	    
	    //Path qualifiedPath = new Path("har://", input_file.toUri() +
	    //	      Path.SEPARATOR + input_file.getParent().toUri().getPath());
	    Path qualifiedPath = new Path("har://", _file_path.toUri().getPath());
	    	      //Path.SEPARATOR + input_file.getParent().toUri().getPath());
	    _harfs.initialize(qualifiedPath.toUri(), _conf);
	    	    
	    _filesInHar = _harfs.listStatus(qualifiedPath);
	    _imageCount = _filesInHar.length;
		_current_image = 0;
	}
	
	@Override
	public void addImage(InputStream image_stream, ImageType type)
	throws IOException {
		Path src_path = new Path(Path.SEPARATOR, "image_" + _imageCount);
		int hash = HarFileSystem.getHarHash(src_path);
		long writer_startPos = _writer.getPos();
		int filelen = 0;
		// write image to har "part" file
		try {
			_writer.writeInt(type.toValue());
			byte[] data = new byte[image_stream.available()];
			image_stream.read(data);
			_writer.write(data);
			filelen = Integer.SIZE + data.length; 
		} finally {
			image_stream.close();
		}
		Path relPath = new Path(src_path.toUri().getPath());
		String partname = "part-0";
		String value = relPath.toString() + " file " + partname + " " + writer_startPos + " " + filelen + " ";
		
		String towrite = value + "\n";
		indexHash.add(new HARIndexContainer(hash, towrite));
		
		_imageCount++;
	}

	@Override
	public long getImageCount() {
		return _imageCount;
	}
	
	@Override
	protected ImageHeader readHeader() throws IOException {
		if (_filesInHar != null) {
			ImageDecoder decoder = CodecManager.getDecoder(ImageType.fromValue(_cacheType));
			if (decoder == null)
				return null;
			ByteArrayInputStream bis = new ByteArrayInputStream(_cacheData);
			ImageHeader header = decoder.decodeImageHeader(bis);
			bis.close();
			return header;
		}
		return null;
	}

	@Override
	protected FloatImage readImage() throws IOException {
		if (_filesInHar != null) {
			ImageDecoder decoder = CodecManager.getDecoder(ImageType.fromValue(_cacheType));
			if (decoder == null)
				return null;
			ByteArrayInputStream bis = new ByteArrayInputStream(_cacheData);
			FloatImage image = decoder.decodeImage(bis);
			bis.close();
			return image;
		}
	   return null;
	}

	@Override
	protected boolean prepareNext() {
		try {
			if (_current_image < _imageCount) {
				_reader = _harfs.open(_filesInHar[_current_image].getPath());
				_cacheType = (int)(_reader.readInt());
				_cacheData = new byte[_reader.available()];
				_reader.read(_cacheData);
				_current_image++;
				return true;
			} else {
				return false;
			}
		} catch (IOException e) {
			return false;
		}
	}

	@Override
	public void close() throws IOException {
		if (_reader != null) {
			_reader.close();
			_harfs.close();
		}
		if (_writer != null) {
			closeIndex();
			_writer.close();
		}
	}
	
	
	private void closeIndex() throws IOException{
		//write "root" dir for har
		Path relPath = new Path(Path.SEPARATOR);
		String toWrite = relPath.toUri().getPath() + " dir none 0 0";
		for(int i = 0; i < _imageCount; i++) {
			toWrite += " image_" + i;
		}
		toWrite += "\n";
		indexHash.add(new HARIndexContainer(HarFileSystem.getHarHash(relPath), toWrite));
		
		
		// try to create index files
		Path masterIndex = new Path(_file_path, "_masterindex");
		Path index = new Path(_file_path, "_index");
		FileSystem fs = masterIndex.getFileSystem(_conf);
		if (fs.exists(masterIndex)) {
			fs.delete(masterIndex, false);
		}
		if (fs.exists(index)) {
			fs.delete(index, false);
		}
		indexStream = fs.create(index);
		masterIndexStream = fs.create(masterIndex);
		String version = HarFileSystem.VERSION + " \n";
		masterIndexStream.write(version.getBytes());
		
		long startPos = 0;
		long startIndexHash = 0;
		long endIndexHash = 0;
		 //write the last part of the master index.
		// hash values must be sorted before writing indexes (normally this is done automatically by MapReduce)
		Collections.sort(indexHash, new HARIndexContainerSorter());
		int i = 0;
		int hash = 0;
		for(; i < indexHash.size(); i++){
			indexStream.write(indexHash.get(i).index_output.getBytes());
			hash = indexHash.get(i).hash;
			if (i >0 && i%1000 == 0) {
				// every 1000 indexes we add a master index entry
				endIndexHash = hash;
				String masterWrite = startIndexHash + " " + endIndexHash + " " + startPos 
				+  " " + indexStream.getPos() + " \n" ;
				masterIndexStream.write(masterWrite.getBytes());
				startPos = indexStream.getPos();
				startIndexHash = endIndexHash;
			}
		}
		
		if (i > 0 && i%1000 > 0) {
			String masterWrite = startIndexHash + " " + hash + " " + startPos  +
			" " + indexStream.getPos() + " \n";
			masterIndexStream.write(masterWrite.getBytes());
		}
		// close the streams
		masterIndexStream.close();
		indexStream.close();
	}
}
