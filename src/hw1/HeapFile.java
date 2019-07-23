package hw1;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * A heap file stores a collection of tuples. It is also responsible for managing pages.
 * It needs to be able to manage page creation as well as correctly manipulating pages
 * when tuples are added or deleted.
 * @author Sam Madden modified by Doug Shook
 *
 */
public class HeapFile {
	
	public static final int PAGE_SIZE = 4096;
	
	/**
	 * Creates a new heap file in the given location that can accept tuples of the given type
	 * @param f location of the heap file
	 * @param types type of tuples contained in the file
	 */
	private File f;
	private TupleDesc type;
	public HeapFile(File f, TupleDesc type) {
		//your code here
		this.f = f;
		this.type = type;
	}
	
	public File getFile() {
		//your code here
		return this.f;
	}
	
	public TupleDesc getTupleDesc() {
		//your code here
		return this.type;
	}
	
	/**
	 * Creates a HeapPage object representing the page at the given page number.
	 * Because it will be necessary to arbitrarily move around the file, a RandomAccessFile object
	 * should be used here.
	 * @param id the page number to be retrieved
	 * @return a HeapPage at the given page numbe
	 */
	public HeapPage readPage(int id) {
		//your code here
		File f = getFile();
		byte[] data = new byte[PAGE_SIZE];
		try {
			RandomAccessFile randF = new RandomAccessFile(f, "r");
			randF.seek((long)(id*PAGE_SIZE));
			randF.read(data, 0, PAGE_SIZE);
			randF.close();
			return new HeapPage(id, data, getId());
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * Returns a unique id number for this heap file. Consider using
	 * the hash of the File itself.
	 * @return
	 */
	public int getId() {
		//your code here
		return this.f.hashCode();
	}
	
	/**
	 * Writes the given HeapPage to disk. Because of the need to seek through the file,
	 * a RandomAccessFile object should be used in this method.
	 * @param p the page to write to disk
	 */
	public void writePage(HeapPage p) {
		//your code here
		File f = getFile();
		byte[] data = p.getPageData();
		int id = p.getId();
		try {
			RandomAccessFile randF = new RandomAccessFile(f, "rw");
			randF.seek(id*PAGE_SIZE);
			randF.write(data, 0, PAGE_SIZE);
			randF.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Adds a tuple. This method must first find a page with an open slot, creating a new page
	 * if all others are full. It then passes the tuple to this page to be stored. It then writes
	 * the page to disk (see writePage)
	 * @param t The tuple to be stored
	 * @return The HeapPage that contains the tuple
	 */
	public HeapPage addTuple(Tuple t) {
		//your code here 
		int numHeapPage = getNumPages();
		for(int i=0; i<numHeapPage; i++) {
			HeapPage p = readPage(i);
			int headerSize = p.getNumSlots();
			for(int j=0; j<headerSize; j++) {
				if(!p.slotOccupied(j)) {
					try {
						p.addTuple(t);
						return p;
					} catch (Exception e) {
						e.printStackTrace();
						
					}
				}
			}
		}
		try {
			HeapPage newPage = new HeapPage(numHeapPage, new byte[PAGE_SIZE], getId());
			numHeapPage ++;
			newPage.addTuple(t);
			return newPage;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * This method will examine the tuple to find out where it is stored, then delete it
	 * from the proper HeapPage. It then writes the modified page to disk.
	 * @param t the Tuple to be deleted
	 */
	public void deleteTuple(Tuple t){
		//your code here
		int pageId = t.getPid();
		HeapPage p = readPage(pageId);
		try {
			p.deleteTuple(t);
			writePage(p);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public HeapPage deleteTupleWithReturn(Tuple t) {
		int pageId = t.getPid();
		HeapPage p = readPage(pageId);
		try {
			p.deleteTuple(t);
			return p;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * Returns an ArrayList containing all of the tuples in this HeapFile. It must
	 * access each HeapPage to do this (see iterator() in HeapPage)
	 * @return
	 */
	public ArrayList<Tuple> getAllTuples() {
		//your code here
		ArrayList<Tuple> allTuples = new ArrayList<>();
		int numHeapPage = getNumPages();
		for(int i=0; i<numHeapPage; i++) {
			Iterator<Tuple> pIter = readPage(i).iterator();
			while(pIter.hasNext()) {
				allTuples.add(pIter.next());
			}
		}
		return allTuples;
	}
	
	/**
	 * Computes and returns the total number of pages contained in this HeapFile
	 * @return the number of pages
	 */
	public int getNumPages() {
		//your code here
		int numPages = (int)(f.length()/PAGE_SIZE);
		return f.length()%PAGE_SIZE == 0 ? numPages:numPages+1;
	}
}
